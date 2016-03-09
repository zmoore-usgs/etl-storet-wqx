show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'install dw new data start time: ' || systimestamp from dual;

begin
	etl_helper_main.install('storet');
	etl_helper_main.update_last_etl(3);
end;
/

select 'install dw new data end time: ' || systimestamp from dual;
