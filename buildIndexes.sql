show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'build indexes start time: ' || systimestamp from dual;

exec etl_helper.create_station_indexes('storet');
exec etl_helper.create_result_indexes('storet');
exec etl_helper.create_station_sum_indexes('storet');
exec etl_helper.create_result_sum_indexes('storet');
exec etl_helper.create_result_ct_sum_indexes('storet');
exec etl_helper.create_result_nr_sum_indexes('storet');

select 'build indexes end time: ' || systimestamp from dual;
