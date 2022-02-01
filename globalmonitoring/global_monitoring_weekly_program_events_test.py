from MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events import *
from mock import patch
from tests.utils import SparkBaseTestCase
from pyspark.storagelevel import StorageLevel

MEMORY_ONLY = StorageLevel(False, True, False, False)
MEMORY_AND_DISK = StorageLevel(True, True, False, False)

diner_order_agg_table_mock = 'diner_order_agg_table'
braze_currents_fact_table_mock = 'braze_currents_fact_table'
order_fact_table_mock = 'order_fact_table'
diner_last_address_uuid_table_mock = 'diner_last_address_uuid_table'
postal_code_dim_table_mock = 'postal_code_dim_table'
market_growth_strategy_definitions_table_mock = 'market_growth_strategy_definitions_table'
braze_campaign_info_table_mock = 'braze_campaign_info_table'
canvas_details_mock = 'canvas_details_table'
diner_churn_mock = 'diner_churn_table'
diner_dim_mock = 'diner_dim_table'


@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.diner_order_agg_table',
       new=diner_order_agg_table_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.braze_currents_fact_table',
       new=braze_currents_fact_table_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.order_fact_table',
       new=order_fact_table_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.diner_last_address_uuid_table',
       new=diner_last_address_uuid_table_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.postal_code_dim_table',
       new=postal_code_dim_table_mock)
@patch(
    'MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.market_growth_strategy_definitions_table',
    new=market_growth_strategy_definitions_table_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.braze_campaign_info_table',
       new=braze_campaign_info_table_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.canvas_details_table',
       new=canvas_details_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.diner_churn_table',
       new=diner_churn_mock)
@patch('MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events.diner_dim_table',
       new=diner_dim_mock)
class TestGlobalMonitoring(SparkBaseTestCase):
    def __init__(self, *args, **kwargs):
        super(TestGlobalMonitoring, self).__init__(*args, **kwargs)

        self.rm_hive_metastore()
        self.table_location_str = '/target_db_location'
        self.rm_output_dir(self.table_location_str)

        self.diner_order_agg_path = self.resources('/CRM/global_monitoring/diner_order_agg/diner_order_agg.csv')
        self.braze_currents_fact_path = self.resources(
            '/CRM/global_monitoring/braze_currents_fact/braze_currents_fact.csv')
        self.order_fact_path = self.resources('/CRM/global_monitoring/order_fact/order_fact.csv')
        self.diner_last_address_uuid_path = self.resources(
            '/CRM/global_monitoring/diner_last_address_uuid/diner_last_address_uuid.csv')
        self.postal_code_dim_path = self.resources('/CRM/global_monitoring/postal_code_dim/postal_code_dim.csv')
        self.market_growth_strategy_definitions_path = self.resources(
            '/CRM/global_monitoring/market_growth_strategy_definitions/market_growth_strategy_definitions.csv')
        self.braze_campaign_info_path = self.resources(
            '/CRM/global_monitoring/braze_campaign_info/braze_campaign_info.csv')
        self.canvas_details_path = self.resources('/CRM/global_monitoring/canvas_details/canvas_details.csv')
        self.diner_churn_path = self.resources('/CRM/global_monitoring/diner_churn/diner_churn.csv')
        self.diner_dim_path = self.resources('/CRM/global_monitoring/diner_dim/diner_dim.csv')

        self.output_location_number_events = self.resources('/target_db_location/global_monitoring/main_table_number_events/')  # where the final mock tables will be saved

        self.run_date = '2019-05-13'
        self.num_cohorts_included = "1"
        self.num_comm_months_included = "1"
        self.env = ''

        self.cl_args = ('--output_location_number_events', self.output_location_number_events,
                        "--run_date", self.run_date,
                        "--num_cohorts_included", self.num_cohorts_included,
                        "--num_comm_months_included", self.num_comm_months_included,
                        "--env", self.env)

    def load_table_csv(self, csv_path, table_name, fm='com.databricks.spark.csv'):
        temp_df = self.sc.read \
            .format(fm) \
            .options(header='true') \
            .option("inferSchema", "true") \
            .load(csv_path)
        temp_df.createOrReplaceTempView(table_name)

    def setUp(self):
        super(TestGlobalMonitoring, self).setUp()

        tables_to_load = [
            (self.diner_order_agg_path, diner_order_agg_table_mock),
            (self.braze_currents_fact_path, braze_currents_fact_table_mock),
            (self.order_fact_path, order_fact_table_mock),
            (self.diner_last_address_uuid_path, diner_last_address_uuid_table_mock),
            (self.postal_code_dim_path, postal_code_dim_table_mock),
            (self.market_growth_strategy_definitions_path, market_growth_strategy_definitions_table_mock),
            (self.braze_campaign_info_path, braze_campaign_info_table_mock),
            (self.canvas_details_path, canvas_details_mock),
            (self.diner_churn_path, diner_churn_mock),
            (self.diner_dim_path, diner_dim_mock)
        ]

        for table in tables_to_load:
            self.load_table_csv(*table)

        self.sc.sql(create_team_marketing_crm_db)

        df = self.sc.sql("select * from %s" % diner_churn_mock)
        df = df.withColumn('start_of_month', df['start_of_month'].cast('date'))
        df.createOrReplaceTempView(diner_churn_mock)

    def tearDown(self):
        super(TestGlobalMonitoring, self).tearDown()

    def test_table_creation(self):
        ## Check pre-built tables:
        df_braze_currents_fact = self.sc.sql("select count(*) from %s" % braze_currents_fact_table_mock)
        self.assertEqual(10, df_braze_currents_fact.collect()[0][0])

        df_diner_order_agg_table_mock = self.sc.sql("select count(*) from %s" % diner_order_agg_table_mock)
        self.assertEqual(11, df_diner_order_agg_table_mock.collect()[0][0])

        df_diner_last_address_uuid_table_mock = self.sc.sql(
            "select count(*) from %s" % diner_last_address_uuid_table_mock)
        self.assertEqual(10, df_diner_last_address_uuid_table_mock.collect()[0][0])

        df_market_growth_strategy_definitions = self.sc.sql(
            "select count(*) from %s" % market_growth_strategy_definitions_table_mock)
        self.assertEqual(11, df_market_growth_strategy_definitions.collect()[0][0])

        df_canvas_details_mock = self.sc.sql("select count(*) from %s" % canvas_details_mock)
        self.assertEqual(7, df_canvas_details_mock.collect()[0][0])

        df_diner_churn_mock = self.sc.sql("select count(*) from %s" % diner_churn_mock)
        self.assertEqual(10, df_diner_churn_mock.collect()[0][0])

        df_diner_dim_mock = self.sc.sql("select count(*) from %s" % diner_dim_mock)
        self.assertEqual(10, df_diner_dim_mock.collect()[0][0])

        ## Call main process:
        cl_args = list(self.cl_args)
        pr = Processor(self.sc, global_monitoring_parse_args(cl_args), self.run_date)
        pr.run_date = self.run_date
        pr.process()

        ## Check newly generated diners based on test data:
        df_audience = self.sc.sql("select count(*) from audience")
        self.assertEqual(10, df_audience.collect()[0][0])

        df_order_type = self.sc.sql("select count(*) from order_type")
        self.assertEqual(10, df_order_type.collect()[0][0])

        df_last_cbsa = self.sc.sql("select count(*) from last_cbsa")
        self.assertEqual(12, df_last_cbsa.collect()[0][0])

        df_braze_unique = self.sc.sql("select count(*) from braze_unique")
        self.assertEqual(4, df_braze_unique.collect()[0][0])

        df_add_tags = self.sc.sql("select count(*) from add_tags")
        self.assertEqual(4, df_add_tags.collect()[0][0])

        df_add_naming = self.sc.sql("select count(*) from add_naming")
        self.assertEqual(4, df_add_naming.collect()[0][0])

        df_vbs = self.sc.sql("select count(*) from vbs")
        self.assertEqual(10, df_vbs.collect()[0][0])

        df_sends = self.sc.sql("select count(*) from final_sends")
        self.assertEqual(4, df_sends.collect()[0][0])
