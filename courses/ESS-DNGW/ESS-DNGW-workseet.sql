select current_User();

alter user DAVIDEMORASCHI set default_role = 'SYSADMIN';
alter user DAVIDEMORASCHI set default_warehouse = 'COMPUTE_WH';
alter user DAVIDEMORASCHI set default_namespace = 'UTIL_DB.PUBLIC';

select current_account();
use role sysadmin;
create database AGS_GAME_AUDIENCE;
drop schema PUBLIC;
create schema RAW;
create table GAME_LOGS (
    RAW_LOG VARIANT
    -- , <col2_name> <col2_type>
    -- supported types: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
    )
    -- comment = '<comment>'
    ;

CREATE STAGE uni_kishore 
  URL = 's3://uni-kishore' 
  DIRECTORY = ( ENABLE = true );

list @uni_kishore/kickoff;

create file format if not exists FF_JSON_LOGS
  --https://docs.snowflake.com/en/sql-reference/sql/create-file-format
    type = JSON
    strip_outer_array = true
    --  [ formatTypeOptions ]
    -- comment = '<comment>'
    ;

select $1
from @uni_kishore/kickoff
(file_format => FF_JSON_LOGS);

copy into ags_game_audience.raw.GAME_LOGS
from @uni_kishore/kickoff
file_format = (format_name=FF_JSON_LOGS);

create or replace view LOGS AS
select 
RAW_LOG:agent::text as agent,
RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
RAW_LOG:user_event::text as user_event,
RAW_LOG:user_login::text as user_login,
*
from ags_game_audience.raw.game_logs;

select * from LOGS;

SELECT current_timestamp();

--what time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';

list @uni_kishore/updated_feed;

select $1
from @uni_kishore/updated_feed
(file_format => FF_JSON_LOGS);

truncate table ags_game_audience.raw.game_logs;
copy into ags_game_audience.raw.GAME_LOGS
from @uni_kishore/updated_feed
file_format = (format_name=FF_JSON_LOGS);

create or replace view LOGS AS
select 
--RAW_LOG:agent::text as agent,
RAW_LOG:ip_address::text as ip_address,
RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
RAW_LOG:user_event::text as user_event,
RAW_LOG:user_login::text as user_login,
*
from ags_game_audience.raw.game_logs
where RAW_LOG:ip_address::text is not null;

select * from LOGS
--where agent is null
WHERE USER_LOGIN ilike '%kishore%'
;

--looking for empty AGENT column
select * 
from ags_game_audience.raw.LOGS
where agent is null;

--looking for non-empty IP_ADDRESS column
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

select distinct user_login
  from LOGS
order by 1;

use role sysadmin;
use database ags_game_audience;
use schema raw;

select parse_ip('8.8.8.8','inet');
select parse_ip('107.217.231.17','inet'):host;

create schema ENHANCED;
use schema ENHANCED;

--Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;