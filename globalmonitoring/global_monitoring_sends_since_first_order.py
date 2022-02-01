import os
import pytz
import argparse
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from MarketingMart.CRM.global_monitoring_tool.global_monitoring_ddls import *

logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s:%(name)s: %(message)s")
root_logger = logging.getLogger("[CRM Global Monitoring]")

DATE_FMT = "%Y-%m-%d"


class Processor:
    def __init__(self, sc, args, run_date):
        self.sc = sc
        try:
            assert args.run_date != ""
            self.run_date = args.run_date
        except Exception:
            self.run_date = run_date
        logging.info("Using run date {}".format(self.run_date))

        self.args = args
        self.target_location_dark_periods = args.output_location_dark_periods  # path to s3 to save table (not the filename)
        self.num_cohorts_included = args.num_cohorts_included
        self.brand = args.brand

    def process(self):

        root_logger.info(
            "=== Starting Global Monitoring Query for Table 2 Job === rundate: {dateid}, brand:{brand}".format(dateid=self.run_date, brand=self.brand))

        initial_day = (datetime.strptime(self.run_date, DATE_FMT) + relativedelta(years=-1)).replace(day=1).strftime(
            DATE_FMT)
        start_fmt = datetime.strptime(initial_day, DATE_FMT)

        root_logger.info(
            "brand: {brand}, initial_day: {initial_day}, type:{type3}".format(brand=self.brand, initial_day=initial_day,
                                                                              type3=type(initial_day)))

        ##### loop to separately grab each cohort of people:
        for index_cohort_month in range(0, int(self.num_cohorts_included)):  ## right-end of interval not included

            starting_date_history, ending_date_history = get_start_end_cohort_dates(start_fmt, index_cohort_month)

            root_logger.info("starting_date_history: {starting_date_history}, type:{type1}  --- "
                             "ending_date_history: {ending_date_history}, type:{type2}  --- ".format(
                starting_date_history=starting_date_history,
                type1=type(starting_date_history),
                ending_date_history=ending_date_history,
                type2=type(ending_date_history)))

            root_logger.info("Running q2 for month-cohort: {start} to {end}".format(start=starting_date_history,
                                                                                    end=ending_date_history))
            write_mode = 'append'
            if index_cohort_month == 0 and self.brand =='seamless':  ## if starting from the first month and SL, drop tables and create them again (note: SL runs first, then runs GH).
                write_mode = 'overwrite'

                drop_table_if_exists = """drop table if exists {global_monitoring_dark_periods_table}""".format(
                    global_monitoring_dark_periods_table=global_monitoring_dark_periods_table)
                self.sc.sql(drop_table_if_exists)
                root_logger.info(" old table dropped")

                self.sc.sql(create_global_monitoring_dark_periods_template.format(
                    global_monitoring_dark_periods_table=global_monitoring_dark_periods_table,
                    location=self.target_location_dark_periods))
                root_logger.info(" new table created")

            root_logger.info(
                "     Getting audience for darkness periods (or periods of time for a diner when they don't receive any communications).....")
            query_initial_audience = """select
                                    distinct 
                                    doa.diner_uuid,
                                    date(first_order_date_utc) as first_order_day,
                                    doa.diner_email,
                                    doa.diner_brand,
                                    case
                                        when lifetime_total_orders = 0 then '0'
                                        when lifetime_total_orders = 1 then '1'
                                        else '>1' end as order_label,
                                    year(first_order_date_utc) as cohort_year,
                                    month(first_order_date_utc) as cohort_month,
                                    case
                                        when last_order_date_utc >= current_date -interval '30'day then 'active' 
                                        else 'inactive' end as active_ind
                                from {diner_order_agg_table} doa
                                JOIN   --- CCPA compliance:
                                        (select 
                                            diner_uuid, 
                                            diner_email, 
                                            diner_brand
                                        from {diner_dim}
                                        where NOT ccpa_deleted_ind
                                        and diner_brand = '{my_brand}')  dd
                                ON doa.diner_uuid = dd.diner_uuid
                                where first_order_date_utc >= date ('{starting_date_history}')
                                      and first_order_date_utc < date ('{ending_date_history}')
                                      and doa.diner_brand = '{my_brand}'
                                      and doa.diner_email not like 'service_guest_email%'
                                      and doa.diner_uuid is not null
                                      and date(as_of_date) = date('{date_id}')  --- partition key in NEW diner_order_agg table: set it up to =TODAY, if job running before 7pm
                                   """
            df_audience_dark = self.sc.sql(query_initial_audience.format(
                diner_order_agg_table=diner_order_agg_table,
                starting_date_history=starting_date_history,
                ending_date_history=ending_date_history,
                diner_dim=diner_dim_table,
                date_id=self.run_date,
                my_brand=self.brand)).persist()

            df_audience_dark.createOrReplaceTempView('audience_dark')
            root_logger.info("number rows in df_audience_dark::::::: {}".format(df_audience_dark.count()))

            root_logger.info("   Getting df_order_type for darkness periods.....")
            query_order_type = """select
                                        diner_uuid,
                                        diner_brand,
                                        case
                                            when delivery_tf = True then 'Delivery'
                                            when delivery_tf = False then 'Pickup'
                                                else null end as first_order_type
                                   from (
                                        select distinct 
                                            aud.diner_uuid,
                                            diner_brand,
                                            first_value(delivery_ind) over (partition by aud.diner_uuid order by order_time_ct asc) as delivery_tf
                                        from {order_fact_table} of
                                            join audience_dark aud
                                                on aud.diner_uuid = of.diner_uuid
                                        where order_month_date >= date ('{starting_date_history}')
                                            and   order_month_date <= date ('{ending_date_history}')  
                                            and order_brand = '{my_brand}' 
                                )
                            """
            df_order_type_dark = self.sc.sql(query_order_type.format(
                order_fact_table=order_fact_table,
                starting_date_history=starting_date_history,
                ending_date_history=ending_date_history,
                my_brand=self.brand)).persist()

            df_order_type_dark.createOrReplaceTempView('order_type_dark')
            root_logger.info("number rows in df_order_type_dark::::::: {}".format(df_order_type_dark.count()))

            root_logger.info("   Getting last_cbsa df for darkness periods.....")
            query_last_cbsa = """select
                                    diner_uuid,
                                    diner_brand,
                                    coalesce(strategy,'Unknown') as strategy
                                from (
                                    select
                                        diner_uuid,
                                        diner_brand,
                                        modified_cbsa_name as cbsa
                                    from (
                                        select
                                            aud.diner_uuid,
                                            diner_brand,
                                            zip
                                        from audience_dark aud
                                        left join {diner_last_address_uuid_table} dla
                                            on aud.diner_uuid  = dla.diner_uuid) z
                                    left join {postal_code_dim_table} pdim
                                        on pdim.postal_code = z.zip ) cb
                                        
                                    left join {market_growth_strategy_definitions_table} sd
                                        on sd.modified_cbsa_name = cb.cbsa
                                    """
            df_last_cbsa_dark = self.sc.sql(
                query_last_cbsa.format(diner_last_address_uuid_table=diner_last_address_uuid_table,
                                       postal_code_dim_table=postal_code_dim_table,
                                       market_growth_strategy_definitions_table=market_growth_strategy_definitions_table)).persist()
            df_last_cbsa_dark.createOrReplaceTempView('last_cbsa_dark')
            root_logger.info("number rows in df_last_cbsa_dark::::::: {}".format(df_last_cbsa_dark.count()))

            root_logger.info("   Getting braze_unique df for darkness periods.....")
            query_braze_events = """
                select distinct
                    aud.diner_brand,
                    diner_uuid,
                    diner_email,
                    event_name,
                    coalesce (canvas_id, campaign_id) as program_id,
                    coalesce (canvas_name, campaign_name) as program_name,
                    dt,
                    datediff(dt, aud.first_order_day)  as days_since_first_order 
                from (  --- add subquery to optimize, because braze_currents_facts is so big:
                        select distinct
                            brand,
                            external_user_id,
                            event_name, 
                            canvas_id, 
                            campaign_id,
                            canvas_name, 
                            campaign_name,
                            dt
                        from {braze_currents_fact_table} 
    
                        where dt >= date ('{starting_date_history}')    
    
                            and event_name in ('users.messages.email.Delivery', 
                                               'users.messages.pushnotification.Send')
                            and coalesce (canvas_name,campaign_name) is not null
                            and brand = '{my_brand}' 
                        ) bcf
                join audience_dark aud
                    on aud.diner_uuid = bcf.external_user_id
                    and aud.diner_brand = bcf.brand  
                    """
            df_braze_unique_dark = self.sc.sql(query_braze_events.format(
                braze_currents_fact_table=braze_currents_fact_table,
                starting_date_history=starting_date_history,
                ending_date_history=ending_date_history,
                my_brand=self.brand)).persist()

            df_braze_unique_dark.createOrReplaceTempView('braze_unique_dark')
            root_logger.info("number rows in df_braze_unique::::::: {}".format(df_braze_unique_dark.count()))

            root_logger.info("   Getting add_tags df for darkness periods.....")
            query_braze_tags = """ 
                        select 
                            diner_brand,
                            diner_uuid,
                            diner_email,
                            case 
                                when event_name = 'users.messages.pushnotification.Send' then 'PushSend' 
                                when event_name = 'users.messages.email.Delivery' then 'EmailDelivery'
                                    else null end as event_name,
                            dt,
                            days_since_first_order,
                            program_name, 
                            coalesce (lower(concat(cast(can.tags as string))) ,  lower(concat(cast(cam.tags as string) )), 'no_tags') as tags,
                            coalesce(program_id, cam.braze_id, can.id) as id
                        from braze_unique_dark bu
                        left join ( 
                                select
                                    bci.braze_id, name, tags
                                from {braze_campaign_info_table} bci
                                join (
                                    select 
                                        braze_id, 
                                        max(updated_at) as dt
                                    from {braze_campaign_info_table}
                                    group by 1) md
                                    on md.braze_id = bci.braze_id
                                        and md.dt = bci.updated_at) cam
                            on cam.braze_id = bu.program_id
                    
                        left join (
                                select
                                    cd.id, name, tags
                                from {canvas_details_table} cd
                                join (
                                    select 
                                        id, 
                                        max(updated_at) as dt
                                    from {canvas_details_table}
                                    group by 1) nd
                                    on nd.id = cd.id
                                        and nd.dt = cd.updated_at) can
                            on can.id = bu.program_id
                        where diner_brand = '{my_brand}' 
                        """
            df_add_tags_dark = self.sc.sql(
                query_braze_tags.format(braze_campaign_info_table=braze_campaign_info_table,
                                        canvas_details_table=canvas_details_table,
                                        my_brand=self.brand)).persist()
            df_add_tags_dark.createOrReplaceTempView('add_tags_dark')

            root_logger.info("   Getting add_naming df for darkness periods.....")
            query_program_names = """ 
                                    select
                                        diner_brand,
                                        diner_uuid,
                                        diner_email,
                                        event_name,
                                        dt,
                                        days_since_first_order,
                                        program_name,
                                        case when tags like '%campus%' then 'Campus'
                                            when tags like '%hana_reactivate%' then null
                                            when tags like '%hana%' then 'HANA'   --- because hana is also tagged as onboarding, also need to go before prospect
                                            when tags like '%nano%' then 'NANO'   --- because hana is also tagged as onboarding, also need to go before prospect
                                            when tags like '%welcome%' then 'Onboarding'    ---- onboarding/welcome need to go before prospect
                                            when tags like '%onboarding%' then 'Onboarding'
                                            when tags like '%prospect%' then 'Prospect'  --- hana and nano need to go before prospect
                                            when tags like '%abandoned cart%' then 'AbandonedCart'
                                            when tags like '%interrupted session%' then 'InterruptedSession'
                                            when tags like '%acquisition%' then 'Acquisition'
                                            when tags like '%bounceback%' then 'Bounceback'
                                            when tags like '%maintain%' then 'Maintain'
                                            when tags like '%triggered/daily%' then 'Daily'  ----  instead of just '%daily%'
                                            when tags like '%dealtrain%' then 'DealTrain'
                                            when tags like '%deal train%' then 'DealTrain'
                                            when tags like '%dotm%' then 'DOTM'   --- needs to be before adhoc
                                            when tags like '%wrap%' then 'WeeklyWrap'    ---- wrap needs to go before enterprise and before adhoc
                                            when tags like '%loyalty%' then 'Loyalty'   ---- loyalty needs to go before enterprise
                                            when tags like '%enterprise%' then 'Enterprise_Partnership'  --- enterprise before adhoc
                                            when tags like '%partnership%' then 'Enterprise_Partnership'
                                            when tags like '%new restaurants%' then 'NewRestos'
                                            when tags like '%/oad%' then 'OAD'
                                            when tags like '%gh+%' then 'GH+/SL+'    --- grubhub+ before pickup and adhoc
                                            when tags like '%sl+%' then 'GH+/SL+'
                                            when tags like '%grubhub+%' then 'GH+/SL+'
                                            when tags like '%seamless+%' then 'GH+/SL+'
                                            when tags like '%pickup%' then 'Pickup'
                                            when tags like '%reactivation%' then 'Reactivation'
                                            when tags like '%reorder%' then 'Reorder'
                                            when tags like '%rtp%' then 'RTP'
                                            when tags like '%sorry%' then 'Sorry'
                                            when tags like '%sweep%' then 'Sweep'
                                            when tags like '%thank you a%' then null
                                            when tags like '%thank you%' then 'ThankYou'
                                            when tags like '%thankyou%' then 'ThankYou'
                                            when tags like '%treat receipt%' then 'TreatReceipt'
                                            when tags like '%treatreceipt%' then 'TreatReceipt'
                                            when tags like '%trending dishes%' then 'TrendingDishes'
                                            when tags like '%trendingdishes%' then 'TrendingDishes'  --- trending dishes before adhoc
                                            when tags like '%weather%' then 'Weather'
                                            when tags like '%wiawiaw%' then 'WIAWIAW'
                                            when tags like '%hot dish%' then 'Hot Dish'
                                            when tags like '%hotdish%' then 'Hot Dish'  -- hotdish before covid
                                            when tags like '%b2b%' then 'B2B'
                                            when tags like '%research%' then 'Research'  ---- before adhoc
                                            when tags like '%app download%' then 'AppDownload' -- app download before covid
                                            when tags like '%appdownload%' then 'AppDownload'
                                            when tags like '%drip%' then 'Drip'
                                            when tags like '%synthetic perk%' then 'SyntheticPerks'
                                            when tags like '%syntheticperk%' then 'SyntheticPerks'
                                            when tags like '%perk alert%' then 'PerkAlert'
                                            when tags like '%perkalert%' then 'PerkAlert'
                                            when tags like '%perk%' then 'Perks--other'  ---- other perk related campaigns
                                            when tags like '%pulse%' then 'Pulse'
                                            when tags like '%covid%' then 'covid'  -- covid before brand but after main programs
                                            when tags like '%brandhol%' then 'BrandHoliday'   --- brand before adhoc
                                            when tags like '%brand%' then 'Brand'
                                            when tags like '%adhoc%' then 'Adhoc'
                                                else 'Other' end as prog_name
                                    from add_tags_dark                 
                                                """
            df_add_naming_dark = self.sc.sql(query_program_names).persist()
            df_add_naming_dark.createOrReplaceTempView('add_naming_dark')
            root_logger.info("number rows in df_add_naming_dark::::::: {}".format(df_add_naming_dark.count()))

            root_logger.info("   Getting final df for dark events.....")
            query_final_events = """ 
                                select
                                    be.diner_brand,
                                    active_ind,
                                    order_label,
                                    first_order_type, 
                                    case when cohort_month between 1 and 9 then concat(cast(cohort_year as string), '_0', cast(cohort_month as string))
                                        else concat(cast(cohort_year as string), '_', cast(cohort_month as string)) 
                                        end as cohort,
                                    strategy,
                                    event_name,
                                    prog_name,
                                    days_since_first_order,
                                    count(distinct be.diner_uuid) as total_sends
                                 from add_naming_dark  be
                                    join last_cbsa_dark lc
                                        on lc.diner_uuid = be.diner_uuid
                                        and lc.diner_brand = be.diner_brand
                                    join order_type_dark ot
                                        on  ot.diner_uuid = be.diner_uuid
                                        and ot.diner_brand = be.diner_brand
                                    join audience_dark aud
                                        on aud.diner_uuid = be.diner_uuid
                                        and aud.diner_brand=be.diner_brand
                                 group by 1,2,3,4,5,6,7,8,9
                             """
            df_final_dark = self.sc.sql(query_final_events).persist()
            df_final_dark.createOrReplaceTempView('final_dark')
            root_logger.info("number rows in df_final_dark::::::: {}".format(df_final_dark.count()))


            ###### write the second table, darkness periods (or periods of time for a diner when they don't receive any communications)
            df_final_dark.write.parquet(
                os.path.join(self.target_location_dark_periods, "date_inserted={}".format(self.run_date)),
                mode=write_mode)
            root_logger.info("    done writing table.")
            self.sc.sql("msck repair table {table}".format(
                table=global_monitoring_dark_periods_table))  # refresh table after adding new partition
            root_logger.info("    done refreshing table.")
            self.sc.catalog.clearCache()
            root_logger.info("    done cleaning cache.")


def get_start_end_cohort_dates(start_fmt, index_cohort_month):
    """
    Given an initial date and the index for the current cohort month,
    it obtains what the starting and end dates should be for the cohort query.
    :param start_fmt: formatted start date for the entire job
    :param index_cohort_month: index for current cohort month
    :return: start and end dates for the current cohort history query
    """
    starting_date = (start_fmt + relativedelta(months=index_cohort_month)).strftime(DATE_FMT)
    ending_date = (start_fmt + relativedelta(months=(index_cohort_month + 1))).strftime(DATE_FMT)

    return starting_date, ending_date


def global_monitoring_parse_args(args):
    description = "Processing data for Global Monitoring"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output_location_dark_periods",
                        help="location for the output hive external table dark periods", required=True)
    parser.add_argument("--run_date",
                        help="run date uses to overwrite. If invalid or None will use current central date", default="",
                        required=False)
    parser.add_argument("--num_cohorts_included",
                        help="number of cohorts to be included in the analysis", required=True)
    parser.add_argument("--env", help="env", required=False)

    parser.add_argument("--brand", help="brand", required=True)

    return parser.parse_args(args)


def main(args):
    current_date_str = datetime.today().strftime('%Y-%m-%d')

    job_name = os.path.splitext(os.path.basename(os.path.abspath(__file__)))[0]

    cl_args = global_monitoring_parse_args(args)

    job_name = '{0}-{1}' \
        .format(job_name, '_'.join(str(datetime.now(pytz.timezone('America/Chicago'))).split(' ')))

    sc = SparkSession \
        .builder \
        .appName(job_name) \
        .enableHiveSupport() \
        .getOrCreate()

    sc.sparkContext.setLogLevel("ERROR")

    logging.info("=== Starting Global Monitoring Data-Grab Job ===")

    processor = Processor(sc, cl_args, run_date=current_date_str)
    processor.process()
    sc.stop()
    logging.info("=== Process Completed, yay! ===")
