show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform project object start time: ' || systimestamp from dual;

prompt dropping storet project_object indexes
exec etl_helper_project_object.drop_indexes('storet');

prompt populating storet project_object
truncate table project_object_swap_storet;

insert /*+ append parallel(4) */
  into project_object_swap_storet(data_source_id,
                                  object_id,
                                  data_source,
                                  organization,
                                  project_identifier,
                                  object_name,
                                  object_type,
                                  object_content)
select '3',
       atobj_uid,
       'STORET',
       organization.org_id organization,
       project.prj_id project_identifier,
       atobj_file_name,
       atobj_type,
       atobj_content
  from wqx.attached_object
       join wqx.organization
         on attached_object.org_uid = organization.org_uid
       join wqx.project
         on attached_object.ref_uid = project.prj_uid
 where tbl_uid = 1;

commit;
select 'Building storet project_object complete: ' || systimestamp from dual;

prompt building storet project_object indexes
exec etl_helper_project_object.create_indexes('storet');

select 'transform project_object end time: ' || systimestamp from dual;
