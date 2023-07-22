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

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
, CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAME_EVENT_LTZ
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

-- Your role should be SYSADMIN
-- Your database menu should be set to AGS_GAME_AUDIENCE
-- The schema should be set to RAW

--a Look Up table to convert from hour number to "time of day name"
create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);
drop table ags_game_audience.raw.time_of_day_lu;
use schema RAW;
--insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;

drop table ags_game_audience.enhanced.logs_enhanced;
--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
create table ags_game_audience.enhanced.logs_enhanced as(
SELECT logs.ip_address
, logs.user_login AS GAMER_NAME
, logs.user_event AS GAME_EVENT_NAME
, logs.datetime_iso8601 AS GAME_EVENT_UTC
, city
, region
, country
, timezone AS GAMER_LTZ_NAME
, CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAME_EVENT_LTZ
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
, tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN raw.time_of_day_lu tod ON tod.hour = EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601))
);

select count(*) from ags_game_audience.enhanced.logs_enhanced;

use database ags_game_audience;
drop schema enhanced;
create schema ENHANCED;
use schema enhanced;
use role sysadmin;
create task load_logs_enhanced
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
    -- <session_parameter> = <value> [ , <session_parameter> = <value> ... ] 
    -- user_task_timeout_ms = <num>
    -- copy grants
    -- comment = '<comment>'
    -- after <string>
  -- when <boolean_expr>
  as
    select 'hello';

--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
use role accountadmin;
use role sysadmin;
grant execute task on account to role SYSADMIN;

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;
--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
drop task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
  as
INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
SELECT logs.ip_address
, logs.user_login AS GAMER_NAME
, logs.user_event AS GAME_EVENT_NAME
, logs.datetime_iso8601 AS GAME_EVENT_UTC
, city
, region
, country
, timezone AS GAMER_LTZ_NAME
, CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAME_EVENT_LTZ
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
, tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN raw.time_of_day_lu tod ON tod.hour = EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601))
;

select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we'll name it _UF
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

--let's truncate so we can start the load over again
-- remember we have that cloned back up so it's fine
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
  as
merge into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
using (
SELECT logs.ip_address
, logs.user_login AS GAMER_NAME
, logs.user_event AS GAME_EVENT_NAME
, logs.datetime_iso8601 AS GAME_EVENT_UTC
, city
, region
, country
, timezone AS GAMER_LTZ_NAME
, CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAME_EVENT_LTZ
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
, tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN raw.time_of_day_lu tod ON tod.hour = EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601))
) r
on r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
and r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
WHEN NOT MATCHED THEN
INSERT(	IP_ADDRESS,
	GAMER_NAME ,
	GAME_EVENT_NAME,
	GAME_EVENT_UTC ,
	CITY ,
	REGION ,
	COUNTRY ,
	GAMER_LTZ_NAME,
	GAME_EVENT_LTZ ,
	DOW_NAME ,
	TOD_NAME )
VALUES(	IP_ADDRESS,
	GAMER_NAME ,
	GAME_EVENT_NAME,
	GAME_EVENT_UTC ,
	CITY ,
	REGION ,
	COUNTRY ,
	GAMER_LTZ_NAME,
	GAME_EVENT_LTZ ,
	DOW_NAME ,
	TOD_NAME );  


use role sysadmin;
use database ags_game_audience;
use schema raw;
CREATE STAGE UNI_KISHORE_PIPELINE 
	URL = 's3://uni-kishore-pipeline' 
	DIRECTORY = ( ENABLE = true );

list @UNI_KISHORE_PIPELINE;

drop table raw.PIPELINE_LOGS;
create table raw.PIPELINE_LOGS 
as select * from raw.game_logs where 1=0;

select count(*) from raw.pipeline_logs;
copy into ags_game_audience.raw.pipeline_logs
from @UNI_KISHORE_PIPELINE
file_format = (format_name=FF_JSON_LOGS);


begin transaction;
copy into ags_game_audience.raw.pipeline_logs
from @UNI_KISHORE_PIPELINE
file_format = (format_name=FF_JSON_LOGS);
commit;
--rollback;

create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	IP_ADDRESS,
	DATETIME_ISO8601,
	USER_EVENT,
	USER_LOGIN,
	RAW_LOG
) as
select 
--RAW_LOG:agent::text as agent,
RAW_LOG:ip_address::text as ip_address,
RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
RAW_LOG:user_event::text as user_event,
RAW_LOG:user_login::text as user_login,
*
from ags_game_audience.raw.game_logs
where RAW_LOG:ip_address::text is not null;

select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
  as
merge into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
using (
SELECT logs.ip_address
, logs.user_login AS GAMER_NAME
, logs.user_event AS GAME_EVENT_NAME
, logs.datetime_iso8601 AS GAME_EVENT_UTC
, city
, region
, country
, timezone AS GAMER_LTZ_NAME
, CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAME_EVENT_LTZ
, DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
, tod.tod_name
from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN raw.time_of_day_lu tod ON tod.hour = EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601))
) r
on r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
and r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
WHEN NOT MATCHED THEN
INSERT(	IP_ADDRESS,
	GAMER_NAME ,
	GAME_EVENT_NAME,
	GAME_EVENT_UTC ,
	CITY ,
	REGION ,
	COUNTRY ,
	GAMER_LTZ_NAME,
	GAME_EVENT_LTZ ,
	DOW_NAME ,
	TOD_NAME )
VALUES(	IP_ADDRESS,
	GAMER_NAME ,
	GAME_EVENT_NAME,
	GAME_EVENT_UTC ,
	CITY ,
	REGION ,
	COUNTRY ,
	GAMER_LTZ_NAME,
	GAME_EVENT_LTZ ,
	DOW_NAME ,
	TOD_NAME );  

create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
  as
copy into ags_game_audience.raw.pipeline_logs
from @UNI_KISHORE_PIPELINE
file_format = (format_name=FF_JSON_LOGS);

execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;