import os
from gdpprojectbuilder import ProjectBuilder
from gdpprojectbuilder.models import RSeriesModel
from MarketingMart import __version__
from MarketingMart.config import *

PROJECT_NAME = "CRM_Global_Monitoring"
CLUSTER_UPTIME_HOURS = 16
CLUSTER_NAME = "_".join((CLUSTER_PREFIX, "global_monitoring"))
CLUSTER_NODE_COUNT = 8
NODE_SIZE = "8xlarge"
CLUSTER_TYPE = 'marketing'

# This job will gather X months worth of communications from X number of cohorts.
# A cohort is defined as the group of diners who placed their first order on that given month (for example, the Febr. 2020 cohort).

num_cohorts_included = 12
num_comm_months_included = 12 ## WARNING: if num months < number cohorts, it wont get all the way until last month of communications!

output_table_number_events_location = "s3://grubhub-gdp-marketing-data-assets-${env}/shared/team_marketing_crm.db/global_monitoring/number_events_new/"
output_table_dark_periods_location = "s3://grubhub-gdp-marketing-data-assets-${env}/shared/team_marketing_crm.db/global_monitoring/dark_periods_new/"

gdp_campaign_input = "global_monitoring_gdp"

run_date = '$(new("org.joda.time.DateTime").toString("yyyy-MM-dd"))'
folder_date = run_date

driver = os.path.join("/usr/local/azkaban_projects", PROJECT_MODULE, "projects", "driver.py")

spark_parameters = '--executor-memory 42G ' \
                    '--num-executors 47 ' \
                    '--executor-cores 10 ' \
                    '--driver-memory 90G ' \
                    '--conf spark.driver.maxResultSize=4G '\
                    '--conf spark.executor.memoryOverhead=4G '\
                    '--deploy-mode client '\
                    '--conf spark.sql.shuffle.partitions=1200 '\
                    '--conf spark.shuffle.service.enabled=true '

project = ProjectBuilder(PROJECT_NAME,
                         __file__,
                         job_module=PROJECT_MODULE,
                         job_module_version=__version__,
                         requirements=True,
                         py_version='python3',
                         team='marketing')

if project.env_config.get("env") == "prod":
    project.add_project_properties("failure.emails", "etl-marketing@grubhub.com, crm-engineering@grubhub.com, "
                                                     "crm-etl-tool-jobs@grubhub.pagerduty.com")
else:
    project.add_project_properties("failure.emails", "jponcelacasasnovas@grubhub.com")

project.add_project_properties('success.emails', 'jponcelacasasnovas@grubhub.com')
project.add_project_properties("job.run_date", run_date)

project.add_bootstrap_action(cluster_name=CLUSTER_NAME,
                             bootstrap_action_name="install crm dependencies",
                             bootstrap_script="install_crm_dependencies_py3.sh",
                             bootstrap_argument_list=[])

cluster_up = project.initialize_flow(cluster_name=CLUSTER_NAME,
                                     model=RSeriesModel,
                                     node_size=NODE_SIZE,
                                     cluster_type=CLUSTER_TYPE,
                                     uptime=CLUSTER_UPTIME_HOURS,
                                     node_count=CLUSTER_NODE_COUNT,
                                     spot_instance=False,
                                     add_ebr=True)

# job for running query 1: weekly_program_events
job_parameters_weekly_program_events = "MarketingMart.CRM.global_monitoring_tool.global_monitoring_weekly_program_events " + \
                                       "--output_location_number_events {loc} ".format(
                                           loc=output_table_number_events_location) + \
                                       "--run_date '{}' ".format("${job.run_date}") + \
                                       "--num_cohorts_included '{num}' ".format(num=num_cohorts_included) + \
                                       "--num_comm_months_included '{num}' ".format(num=num_comm_months_included) + \
                                       "--env {} ".format("${env}")

cmd_weekly_program_events = " ".join(["spark-submit", spark_parameters, driver, job_parameters_weekly_program_events])
job_weekly_program_events = project.add_job(name="global_monitoring_weekly_program_events",
                                            cluster_name=CLUSTER_NAME,
                                            job_type="gdpcommand",
                                            command=cmd_weekly_program_events,
                                            retries=0,
                                            retry_backoff=1,
                                            dependencies=[cluster_up])

# job for running query 2: sends_since_first_order
sub_jobs_sends_since_first = {}
for brand in ['seamless', 'grubhub']:

    ### i need the two brands to run in order, not in paralell:
    if brand == 'seamless':
        depend = [job_weekly_program_events]
    else:
        depend = [sub_jobs_sends_since_first['seamless']]

    job_parameters_sends_since_first_order = "MarketingMart.CRM.global_monitoring_tool.global_monitoring_sends_since_first_order " + \
                                         "--output_location_dark_periods {loc} ".format(
                                             loc=output_table_dark_periods_location) + \
                                         "--run_date '{}' ".format("${job.run_date}") + \
                                         "--num_cohorts_included '{num}' ".format(num=num_cohorts_included) + \
                                         "--brand '{brand}' ".format(brand=brand) + \
                                         "--env {} ".format("${env}")

    cmd_sends_since_first_order = " ".join(
        ["spark-submit", spark_parameters, driver, job_parameters_sends_since_first_order])
    sub_jobs_sends_since_first[brand] = project.add_job(name="global_monitoring_sends_since_first_order_{}".format(brand),
                                                  cluster_name=CLUSTER_NAME,
                                                  job_type="gdpcommand",
                                                  command=cmd_sends_since_first_order,
                                                  retries=0,
                                                  retry_backoff=1,
                                                  dependencies=depend)

oad_processor_cleanup = project.cleanup_flow(flow_name="global_monitoring",
                                             cluster_name=CLUSTER_NAME,
                                             dependencies=[sub_jobs_sends_since_first['grubhub']])

project.generate_project()
