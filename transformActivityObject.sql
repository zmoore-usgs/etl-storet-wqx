show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform activity object start time: ' || systimestamp from dual;

prompt dropping storet activity_object indexes
exec etl_helper_activity_object.drop_indexes('storet');

prompt populating storet activity_object
truncate table activity_object_swap_storet;

insert /*+ append parallel(4) */
  into activity_object_swap_storet (data_source_id,
                                    object_id,
                                    data_source,
                                    activity_id,
                                    organization,
                                    activity,
                                    object_name,
                                    object_type,
                                    object_content)
select '3' data_source_id,
       atobj_uid object_id,
       'STORET' data_source,
       ref_uid,
       activity_swap_storet.organization,
       activity_swap_storet.activity,
       atobj_file_name object_name,
       atobj_type object_type,
       atobj_content object_content
  from wqx.attached_object
       join activity_swap_storet
         on attached_object.ref_uid = activity_swap_storet.activity_id
 where tbl_uid = 3;

commit;
select 'Building storet activity_object complete: ' || systimestamp from dual;

prompt building storet activity_object indexes
exec etl_helper_activity_object.create_indexes('storet');

select 'transform activity_object end time: ' || systimestamp from dual;
