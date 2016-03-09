show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'build summary tables start time: ' || systimestamp from dual;

begin
	etl_helper_summary.create_tables('storet');
	etl_helper_summary.create_indexes('storet');
end;
/

select 'build summary tables end time: ' || systimestamp from dual;
