"""Default TUR DAG entry point.

Replace this file with a custom DAG to fully override TUR, or keep this
delegation to use the YAML-configured default.
"""

from daily_dag_weekday_factory import register_country_dag


TUR_daily_dag_weekday = register_country_dag("TUR")
