import os
import sys
import importlib.resources
from biasanalyzer.models import TemporalEventGroup
from jinja2 import Environment, FileSystemLoader


class CohortQueryBuilder:
    def __init__(self):
        """Get the path to SQL templates, whether running from source or installed."""
        try:
            if sys.version_info >= (3, 9):
                # Python 3.9+: Use importlib.resources.files()
                template_path = importlib.resources.files("biasanalyzer").joinpath("sql_templates")
            else:
                # Python 3.8: Use importlib.resources.path() (context manager)
                with importlib.resources.path("biasanalyzer", "sql_templates") as p:
                    template_path = str(p)
        except ModuleNotFoundError:
            template_path = os.path.join(os.path.dirname(__file__), "sql_templates")

        print(f'template_path: {template_path}')
        self.env = Environment(loader=FileSystemLoader(template_path), extensions=['jinja2.ext.do'])
        self.env.globals.update(
            demographics_filter=self._load_macro('demographics_filter'),
            temporal_event_filter=self.temporal_event_filter
        )

    def _load_macro(self, macro_name):
        """
        Load a macro from macros.sql.j2 into the Jinja2 environment.
        """
        macros_template = self.env.get_template('macros.sql.j2')
        return macros_template.module.__dict__[macro_name]


    def build_query(self, cohort_config: dict) -> str:
        """
        Build a SQL query from the CohortCreationConfig object.

        Args:
            cohort_config: dict object loaded from yaml file for building sql query.

        Returns:
            str: The rendered SQL query.
        """
        template_name = cohort_config.get('template_name')
        inclusion_criteria = cohort_config.get('inclusion_criteria')
        exclusion_criteria = cohort_config.get('exclusion_criteria', {})
        template = self.env.get_template(f"{template_name}.sql.j2")
        return template.render(
            inclusion_criteria=inclusion_criteria,
            exclusion_criteria=exclusion_criteria
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
        event_sql = ""

        if event["event_type"] == "condition_occurrence":
            if "event_instance" in event and event["event_instance"] is not None:
                event_sql = (
                    f"SELECT person_id, event_date FROM ranked_events WHERE condition_concept_id = {event['event_concept_id']} "
                    f"AND event_instance >= {event['event_instance']}"
                )
            else:
                event_sql = (
                    f"SELECT person_id, event_date FROM ranked_events WHERE condition_concept_id = {event['event_concept_id']}"
                )

        elif event["event_type"] == "visit_occurrence":
            if "event_instance" in event and event["event_instance"] is not None:
                event_sql = (
                    f"SELECT person_id, event_date FROM ranked_visits WHERE visit_concept_id = {event['event_concept_id']} "
                    f"AND event_instance >= {event['event_instance']}"
                )
            else:
                event_sql = (
                    f"SELECT person_id, event_date FROM ranked_visits WHERE visit_concept_id = {event['event_concept_id']}"
                )

        return event_sql

    @staticmethod
    def render_event_group(event_group):
        """
        Recursively process a group of events and generate SQL queries.

        Args:
            event_group (dict): Event group containing multiple events or nested event groups.

        Returns:
            str: SQL query string for the event group.
        """
        queries = [] # accumulate SQL queries when called recursively with nested event groups
        if "events" not in event_group:  # Single event
            return CohortQueryBuilder.render_event(event_group)
        else:
            for event in event_group["events"]:
                event_sql = CohortQueryBuilder.render_event_group(event)
                if event_sql:
                    queries.append(event_sql)
            if not queries:
                return ""

            if event_group["operator"] == "AND":
                return f"SELECT person_id FROM ({' INTERSECT '.join(queries)}) AS subquery_and"
            elif event_group["operator"] == "OR":
                return f"SELECT person_id FROM ({' UNION '.join(queries)}) AS subquery_or"
            elif event_group["operator"] == "NOT":
                if queries[0].startswith('SELECT person_id, event_date'):
                    queries[0] = queries[0].replace('SELECT person_id, event_date', 'SELECT person_id', 1)
                table_name = event_group["events"][0]['event_type']
                return f"SELECT person_id FROM {table_name} WHERE person_id NOT IN ({queries[0]})"
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
                            return f"{queries[0]} AND event_date > DATE '{timestamp}'"
                        else:  # non-timestamp event needs to happen before timestamp
                            return f"{queries[0]} AND event_date < DATE '{timestamp}'"
                    else:
                        print(f"This should not happen: event_group: {event_group} with BEFORE operator only "
                              f"has one query event {queries}")
                        return ''
                elif len(queries) == 2:
                    for i in range(len(queries)):
                        if (queries[i].startswith('SELECT person_id')
                                and (not queries[i].startswith('SELECT person_id, event_date'))):
                            queries[i] = queries[i].replace('SELECT person_id', 'SELECT person_id, event_date', 1)

                    event_group = TemporalEventGroup(**event_group)
                    interval_sql = event_group.get_interval_sql()
                    return f"""
                            SELECT person_id  
                            FROM ({queries[0]}) e1 
                            WHERE EXISTS (
                                SELECT 1 FROM ({queries[1]}) e2
                                WHERE e1.person_id = e2.person_id
                                AND e1.event_date < e2.event_date
                                {interval_sql}                                    
                            )
                            """
            return ""

    def temporal_event_filter(self, event_groups, alias='c'):
        """
        Generates the SQL filter for temporal event criteria.

        Args:
            event_groups (list): List of event groups (dictionaries) to be processed.
            alias (str): Alias for the table name to use for filtering.
            Default is 'c' representing condition_occurrence table.
        Returns:
            str: SQL filter for temporal event selection.
        """
        filters = []
        for event_group in event_groups:
            group_sql = self.render_event_group(event_group)
            if group_sql:
                filters.append(f"AND {alias}.person_id IN ({group_sql})")

        return " ".join(filters) if filters else ""
