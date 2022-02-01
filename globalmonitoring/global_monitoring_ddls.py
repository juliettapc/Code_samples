from MarketingMart.ddls import *

## input tables:
canvas_details_table = '{db}.{table}'.format(db=source_marketing_vendor_db, table='canvas_details')
braze_campaign_info_table = '{db}.{table}'.format(db=customer_engagement_db, table='braze_campaign_info')
market_growth_strategy_definitions_table = '{db}.{table}'.format(db=finance_reporting_db, table='market_growth_strategy_definitions')

## output tables
global_monitoring_number_sends_table = '{db}.{table}'.format(db=team_marketing_crm_db, table='global_monitoring_number_sends_new')
global_monitoring_dark_periods_table = '{db}.{table}'.format(db=team_marketing_crm_db, table='global_monitoring_dark_periods_new')

create_global_monitoring_number_sends_template = """
CREATE EXTERNAL TABLE  {global_monitoring_number_sends_table} (
diner_brand string,
vbs_bucket string,
active_ind string,
order_label string,
first_order_type string,
cohort string,
strategy string,
event_year integer,
event_month date,
event_week date,
event string,
prog_name string,
num_events integer,
num_diner integer
)partitioned by (date_inserted date)
STORED AS PARQUET
LOCATION
'{location}'
TBLPROPERTIES ('parquet.compress'='snappy')
""".format(global_monitoring_number_sends_table=global_monitoring_number_sends_table, location='{location}')

create_global_monitoring_dark_periods_template = """
CREATE EXTERNAL TABLE  {global_monitoring_dark_periods_table} (
    diner_brand string,
    active_ind string,
    order_label string,
    first_order_type string,
    cohort string,
    strategy string,
    event_name string,
    prog_name string,
    days_since_first_order integer,
    total_sends integer
)partitioned by (date_inserted date)
STORED AS PARQUET
LOCATION
'{location}'
TBLPROPERTIES ('parquet.compress'='snappy')
""".format(global_monitoring_dark_periods_table=global_monitoring_dark_periods_table, location='{location}')
