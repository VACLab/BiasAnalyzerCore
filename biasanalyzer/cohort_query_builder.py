import os
from jinja2 import Environment, FileSystemLoader


class CohortQueryBuilder:
    def __init__(self):
        template_path = os.path.join(os.path.dirname(__file__), "..", 'sql_templates')
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
        template = self.env.get_template(f"{template_name}.sql")
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
                    f"SELECT person_id FROM ranked_events WHERE condition_concept_id = {event['event_concept_id']} "
                    f"AND event_instance >= {event['event_instance']}"
                )
            else:
                event_sql = (
                    f"SELECT person_id FROM ranked_events WHERE condition_concept_id = {event['event_concept_id']}"
                )

        elif event["event_type"] == "visit_occurrence":
            if "event_instance" in event and event["event_instance"] is not None:
                event_sql = (
                    f"SELECT person_id FROM ranked_visits WHERE visit_concept_id = {event['event_concept_id']} "
                    f"AND event_instance >= {event['event_instance']}"
                )
            else:
                event_sql = (
                    f"SELECT person_id FROM ranked_visits WHERE visit_concept_id = {event['event_concept_id']}"
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
        event_queries = []

        if "events" not in event_group:  # Single event
            return CohortQueryBuilder.render_event(event_group)

        for event in event_group["events"]:
            event_sql = CohortQueryBuilder.render_event_group(event)
            if event_sql:
                event_queries.append(event_sql)

        if not event_queries:
            return ""

        if event_group["operator"] == "AND":
            return f"SELECT person_id FROM ({' INTERSECT '.join(event_queries)})"
        elif event_group["operator"] == "OR":
            return f"SELECT person_id FROM ({' UNION '.join(event_queries)})"

        return ""

    def temporal_event_filter(self, event_groups):
        """
        Generates the SQL filter for temporal event criteria.

        Args:
            event_groups (list): List of event groups (dictionaries) to be processed.

        Returns:
            str: SQL filter for temporal event selection.
        """
        filters = []
        print(f'event_groups: {event_groups}', flush=True)
        for event_group in event_groups:
            group_sql = self.render_event_group(event_group)
            if group_sql:
                filters.append(f"AND c.person_id IN ({group_sql})")

        return " ".join(filters) if filters else ""
