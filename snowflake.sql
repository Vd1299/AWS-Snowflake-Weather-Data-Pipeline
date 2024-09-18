CREATE WAREHOUSE IF NOT EXISTS dbt_wh WITH warehouse_size='x-small';
CREATE DATABASE DE_PROJECT;
CREATE ROLE IF NOT EXISTS dbt_role;

show grants on warehouse dbt_wh;

grant usage on warehouse dbt_wh to role dbt_role;
grant all on database DE_PROJECT to role dbt_role;
grant role dbt_role to user "your snowflake user name";
use role dbt_role;
USE DATABASE DE_PROJECT;

CREATE or replace TABLE weather_data(
    temp       NUMBER(20,0),
    CITY          VARCHAR(128) 
    ,humidity   NUMBER(20,5)
    ,wind_speed      NUMBER(20,5) 
   ,time             VARCHAR(128)  
   ,wind_dir        VARCHAR(128)
   ,pressure_mb    NUMBER(20,5)
);

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'your IAM arn'
  storage_allowed_locations = ('your s3 URI');

DESC INTEGRATION s3_int;
-- copy your iam role arn and your external ID and paste it in trust relationship aws arn and add in a condition for externalid in the new aws2snowflake IAM role created.
create or replace file format csv_format
                    type = csv
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;
                    
create or replace stage ext_csv_stage
  URL = 'your s3 URI'
  STORAGE_INTEGRATION = s3_int
  file_format = csv_format;

create or replace pipe mypipe auto_ingest=true as
copy into weather_data
from @ext_csv_stage
on_error = CONTINUE;

show pipes;
-- copy notification channel go to s3 buckets properties create event, select event type, select sqs topic and provide the arn copied from notification channel

select * from weather_data;