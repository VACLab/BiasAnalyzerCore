import os
import sys
import importlib.resources
from biasanalyzer.models import TemporalEventGroup, DOMAIN_MAPPING
from jinja2 import Environment, FileSystemLoader


class CohortQueryBuilder:
    def __init__(self, cohort_creation=True):
        """Get the path to SQL templates, whether running from source or installed."""
        try:
            if sys.version_info >= (3, 9): # pragma: no cover
                # Python 3.9+: Use importlib.resources.files()
                template_path = importlib.resources.files("biasanalyzer").joinpath("sql_templates")
            else:
                # Python 3.8: Use importlib.resources.path() (context manager)
                with importlib.resources.path("biasanalyzer", "sql_templates") as p:
                    template_path = str(p)
        except ModuleNotFoundError: # pragma: no cover
            template_path = os.path.join(os.path.dirname(__file__), "sql_templates")

        print(f'template_path: {template_path}, cohort_creation: {cohort_creation}')
        self.env = Environment(loader=FileSystemLoader(template_path), extensions=['jinja2.ext.do'])
        if cohort_creation:
            self.env.globals.update(
                demographics_filter=self._load_macro('demographics_filter'),
                temporal_event_filter=self.temporal_event_filter
            )

    def _extract_domains(self, events):
        domains = set()
        for event in events:
            if "event_type" in event and event["event_type"] != "date":
                domains.add(event["event_type"])
            if "events" in event:
                domains.update(self._extract_domains(event["events"]))
        return domains

    def _load_macro(self, macro_name):
        """
        Load a macro from macros.sql.j2 into the Jinja2 environment.
        """
        macros_template = self.env.get_template('macros.sql.j2')
        return macros_template.module.__dict__[macro_name]

    def build_query_cohort_creation(self, cohort_config: dict) -> str:
        """
        Build a SQL query from the CohortCreationConfig object.
        :param cohort_config: dict object loaded from yaml file for building sql query.
        :return: The rendered SQL query.
        """
        inclusion_criteria = cohort_config.get('inclusion_criteria')
        exclusion_criteria = cohort_config.get('exclusion_criteria', {})
        inclusion_events = inclusion_criteria.get("temporal_events", [])
        exclusion_events = exclusion_criteria.get("temporal_events", [])
        temporal_events = bool(inclusion_events) # Only inclusion_events matter for cohort dates
        all_domains = self._extract_domains(inclusion_events + exclusion_events)
        # Filter DOMAIN_MAPPING to exclude domains with table: None
        valid_domains = {k: v for k, v in DOMAIN_MAPPING.items() if v.get('table')}
        ranked_domains = {dt: valid_domains[dt] for dt in all_domains if dt in valid_domains}

        if not temporal_events:
            # For demographic only inclusion criteria, filter DOMAIN_MAPPING to exclude domains with table: None
            ranked_domains = valid_domains

        template = self.env.get_template(f"cohort_creation_query.sql.j2")
        return template.render(
            inclusion_criteria=inclusion_criteria,
            exclusion_criteria=exclusion_criteria,
            ranked_domains=ranked_domains,
            temporal_events=temporal_events
        )

    def build_concept_prevalence_query(self, concept_type: str, cid: int, filter_count: int, vocab: str) -> str:
        """
        Build a SQL query for concept prevalence statistics for a given domain and cohort.
        :param concept_type: Domain from DOMAIN_MAPPING (e.g., 'condition_occurrence').
        :param cid: Cohort definition ID.
        :param filter_count: Minimum count threshold for concepts with 0 meaning no filtering
        :param vocab: Vocabulary ID. Defaults to domain-specific vocabulary as defined in DOMAIN_MAPPING if set to None
        :return: The rendered SQL query
        :raises ValueError if concept_type is not invalid
        """

        # Validate concept_type
        if concept_type not in DOMAIN_MAPPING or DOMAIN_MAPPING[concept_type]["table"] is None:
            valid_domains = [k for k in DOMAIN_MAPPING.keys() if DOMAIN_MAPPING[k]["table"] is not None]
            raise ValueError(f"Invalid concept_type: {concept_type}. Must be one of {valid_domains}")

        # The provided vocab is assumed to be already validated if it is not set to None. Otherwise,
        # if set to None, use domain-specific default vocabulary
        effective_vocab = vocab if vocab is not None else DOMAIN_MAPPING[concept_type]["default_vocab"]
        # Load and render the template
        template = self.env.get_template("cohort_concept_prevalence_query.sql.j2")
        return template.render(
            table_name=DOMAIN_MAPPING[concept_type]["table"],
            concept_id_column=DOMAIN_MAPPING[concept_type]["concept_id"],
            start_date_column=DOMAIN_MAPPING[concept_type]["start_date"],
            cid=cid,
            filter_count=filter_count,
            vocab=effective_vocab
        )

    @staticmethod
    def render_event(event):
        """
        Generate SQL query for an individual event.

        Args:
            event (dict): Event dictionary with keys like 'event_type', 'event_concept_id', and 'event_instance'.

        Returns:
            str: SQL query string for the event.
        """
        domain = DOMAIN_MAPPING.get(event["event_type"])
        if not domain or not domain["table"]:
            return ""

        base_sql = f"SELECT person_id, event_start_date, event_end_date FROM ranked_events_{event['event_type']}"
        conditions = [f"concept_id = {event['event_concept_id']}"]
        if "event_instance" in event and event["event_instance"] is not None:
            conditions.append(f"event_instance >= {event['event_instance']}")

        return f"{base_sql} WHERE {' AND '.join(conditions)}"


    @staticmethod
    def render_event_group(event_group, alias_prefix="evt"):
        """
        Recursively process a group of events and generate SQL queries.

        Args:
            event_group (dict): Event group containing multiple events or nested event groups.
            alias_prefix (str): Prefix for generating unique aliases.
        Returns:
            str: SQL query string for the event group.
        """
        queries = [] # accumulate SQL queries when called recursively with nested event groups
        if "events" not in event_group:  # Single event
            return CohortQueryBuilder.render_event(event_group)
        else:
            for i, event in enumerate(event_group["events"]):
                event_sql = CohortQueryBuilder.render_event_group(event, f"{alias_prefix}_{i}")
                if event_sql:
                    queries.append(event_sql)
            if not queries: # pragma: no cover
                return ""

            if event_group["operator"] == "AND":
                if len(queries) == 1:
                    return queries[0]
                # First, get person_ids that satisfy all conditions
                person_id_sql = f"""
                    SELECT a.person_id
                    FROM ({queries[0]}) AS a"""
                for i, query in enumerate(queries[1:], 1):
                    person_id_sql += f"""
                        JOIN (
                            SELECT DISTINCT person_id
                            FROM ({query}) AS b{i}
                        ) AS b{i}
                        ON a.person_id = b{i}.person_id
                    """
                # Then, union all events for qualifying person_ids
                combined_sql = f"""
                    SELECT person_id, event_start_date, event_end_date
                    FROM (
                        {' UNION ALL '.join(f'({q})' for q in queries)}
                    ) AS all_events
                    WHERE person_id IN (
                        {person_id_sql}
                    )
                """
                return combined_sql

            elif event_group["operator"] == "OR":
                return f"SELECT person_id, event_start_date, event_end_date FROM ({' UNION '.join(queries)}) AS {alias_prefix}_or"
            elif event_group["operator"] == "NOT":
                not_query = queries[0]
                # Return a query that selects all persons from a base table (e.g., person),
                # excluding those in the NOT subquery, while allowing dates from other criteria
                return f"""
                        SELECT p.person_id, NULL AS event_start_date, NULL AS event_end_date
                        FROM person p
                        WHERE p.person_id NOT IN (
                            SELECT person_id FROM ({not_query}) AS {alias_prefix}_not
                        )
                    """
            elif event_group["operator"] == "BEFORE":
                if len(queries) == 1:
                    # the other query is the timestamp event which has to be handled here as it depends on the other
                    # event in the BEFORE operator
                    timestamp_event = next((e for e in event_group['events'] if e["event_type"] == "date"), None)
                    non_timestamp_event = next((e for e in event_group['events'] if e["event_type"] != "date"), None)
                    if timestamp_event and non_timestamp_event:
                        timestamp = timestamp_event["timestamp"]
                        timestamp_event_index = event_group['events'].index(timestamp_event)
                        non_timestamp_event_index = event_group['events'].index(non_timestamp_event)
                        if timestamp_event_index < non_timestamp_event_index:
                            # timestamp needs to happen before non-timestamp event
                            return f"""
                                        SELECT person_id, event_start_date, event_end_date
                                        FROM ({queries[0]}) AS {alias_prefix}_0
                                        WHERE event_start_date > DATE '{timestamp}'
                                    """
                        else:
                            # non-timestamp event needs to happen before timestamp
                            return f"""
                                        SELECT person_id, event_start_date, event_end_date
                                        FROM ({queries[0]}) AS {alias_prefix}_0
                                        WHERE event_start_date < DATE '{timestamp}'
                                    """
                elif len(queries) == 2:
                    event_group = TemporalEventGroup(**event_group)
                    e1_alias = f"e1_{alias_prefix}"
                    e2_alias = f"e2_{alias_prefix}"
                    interval_sql = event_group.get_interval_sql(e1_alias=e1_alias, e2_alias=e2_alias)

                    # Ensure both events contribute dates with temporal order and interval
                    return f"""
                                SELECT {e1_alias}.person_id, {e1_alias}.event_start_date, {e1_alias}.event_end_date
                                    FROM ({queries[0]}) AS {e1_alias}
                                    JOIN ({queries[1]}) AS {e2_alias}
                                    ON {e1_alias}.person_id = {e2_alias}.person_id
                                    AND {e1_alias}.event_start_date < {e2_alias}.event_start_date
                                    {interval_sql}
                                    UNION ALL
                                    SELECT {e2_alias}.person_id, {e2_alias}.event_start_date, {e2_alias}.event_end_date
                                    FROM ({queries[1]}) AS {e2_alias}
                                    JOIN ({queries[0]}) AS {e1_alias}
                                    ON {e2_alias}.person_id = {e1_alias}.person_id
                                    AND {e1_alias}.event_start_date < {e2_alias}.event_start_date
                                    {interval_sql}
                            """
            return ""  # pragma: no cover

    def temporal_event_filter(self, event_groups, alias='c'):
        """
        Generates the SQL filter for temporal event criteria.

        Args:
            event_groups (list): List of event groups (dictionaries) to be processed.
            alias (str): Alias for the table name to use for filtering.
            Default is 'c' representing condition_occurrence table or condition_qualifying_event CTE.
            'ex' alias represents the exclusion criteria filtering
        Returns:
            str: SQL filter for temporal event selection.
        """
        filters = []
        for i, event_group in enumerate(event_groups):
            group_sql = self.render_event_group(event_group)
            if group_sql:
                if alias == 'ex':
                    # exclusion criteria
                    filters.append(f"AND {alias}.person_id IN (SELECT person_id FROM ({group_sql}) AS ex_subquery_{i})")
                else:
                    filters.append(f"({group_sql})")
        if not filters:  # pragma: no cover
            return ""
        if alias == 'ex':
            # For exclusion, combine with AND as filters
            return " ".join(filters)
        else:
            # For inclusion, handle both single event group case with operator defined and multiple event group
            # case with no operator defined
            if len(filters) > 1:
                # For multiple temporal event group case with no operator defined, use "OR" operator by default
                # An example YAML block for multiple temporal event group is shown below for reference, in which
                # case, patients who satisfy either group (condition 37311061 or drug 67890) will be included:
                # inclusion_criteria:
                #   temporal_events:
                #     - operator: AND
                #       events:
                #         - event_type: condition_occurrence
                #           event_concept_id: 37311061
                #     - operator: AND
                #       events:
                #         - event_type: drug_exposure
                #           event_concept_id: 67890
                return (f"SELECT person_id, event_start_date, event_end_date FROM "
                        f"({' UNION ALL '.join(filters)}) AS combined_events")

            # Single event group case with operator defined
            return filters[0]
