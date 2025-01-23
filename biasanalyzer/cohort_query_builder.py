import os
from jinja2 import Environment, FileSystemLoader


class CohortQueryBuilder:
    def __init__(self):
        template_path = os.path.join(os.path.dirname(__file__), "..", 'sql_templates')
        self.env = Environment(loader=FileSystemLoader(template_path), extensions=['jinja2.ext.do'])
        self.env.globals.update(
            demographics_filter=self._load_macro('demographics_filter'),
            handle_operator=self._load_macro('handle_operator'),
            temporal_event_filter=self._load_macro('temporal_event_filter')
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
