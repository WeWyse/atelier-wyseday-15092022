#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# [END import_module]

query1 = [
   """create or replace TABLE WEWYSE_DATABASE.PUBLIC.PLAYERTEST3 (PLAYER_ID VARCHAR(16777216),PLAYER_SLUG VARCHAR(16777216),FIRST_NAME VARCHAR(16777216),LAST_NAME VARCHAR(16777216),PLAYER_URL VARCHAR(16777216),FLAG_CODE VARCHAR(16777216),RESIDENCE VARCHAR(16777216),BIRTHPLACE VARCHAR(16777216),BIRTHDATE VARCHAR(16777216),BIRTH_YEAR VARCHAR(16777216),BIRTH_MONTH NUMBER(38,0),BIRTH_DAY NUMBER(38,0),TURNED_PRO NUMBER(38,0),WEIGHT_LBS NUMBER(38,0),WEIGHT_KG NUMBER(38,0),HEIGHT_FT VARCHAR(16777216),HEIGHT_INCHES NUMBER(38,0),HEIGHT_CM NUMBER(38,0),HANDEDNESS VARCHAR(16777216),BACKHAND VARCHAR(16777216));"""
]


# [START instantiate_dag]
with DAG(
    'snowflake_connector_create',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'enable_xcom_pickling' : True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description='A simple tutorial DAG airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example-snowflake-create'],
) as dag:
    # [END instantiate_dag]

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = SnowflakeOperator(
        task_id='snowflake_task1',
        sql=query1,
        snowflake_conn_id='snowflake_id'
    )
    # [END basic_task]

    t1 >> t2 
# [END tutorial]
