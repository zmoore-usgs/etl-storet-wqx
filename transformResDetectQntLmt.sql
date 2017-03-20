show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform res_detect_qnt_limit start time: ' || systimestamp from dual;

prompt dropping storet res_detect_qnt_lmt indexes
exec etl_helper_res_detect_qnt_lmt.drop_indexes('storet');

prompt populating storet res_detect_qnt_lmt
truncate table res_detect_qnt_lmt_swap_storet;

insert /*+ append parallel(4) */
  into res_detect_qnt_lmt_swap_storet(data_source_id, data_source, station_id, site_id, event_date, activity, analytical_method,
                                      characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,
                                      organization_name, project_id, assemblage_sampled_name, sample_tissue_taxonomic_name, activity_id,
                                      result_id, detection_limit_id, detection_limit, detection_limit_unit, detection_limit_desc)
select /*+ parallel(4) */
       result_swap_storet.data_source_id,
       result_swap_storet.data_source,
       result_swap_storet.station_id,
       result_swap_storet.site_id,
       result_swap_storet.event_date,
       result_swap_storet.activity,
       result_swap_storet.analytical_method,
       result_swap_storet.characteristic_name,
       result_swap_storet.characteristic_type,
       result_swap_storet.sample_media,
       result_swap_storet.organization,
       result_swap_storet.site_type,
       result_swap_storet.huc,
       result_swap_storet.governmental_unit_code,
       result_swap_storet.organization_name,
       result_swap_storet.project_id,
       result_swap_storet.assemblage_sampled_name,
       result_swap_storet.sample_tissue_taxonomic_name,
       result_swap_storet.activity_id,
       result_swap_storet.result_id,
       wqx_res_detect_qnt_lmt.rdqlmt_uid,
       wqx_res_detect_qnt_lmt.rdqlmt_measure,
       wqx_res_detect_qnt_lmt.msunt_cd,
       wqx_res_detect_qnt_lmt.dqltyp_name
  from wqx_res_detect_qnt_lmt
       join result_swap_storet
         on wqx_res_detect_qnt_lmt.res_uid = result_swap_storet.result_id;
commit;

select 'Building res_detect_qnt_lmt_swap_storet complete: ' || systimestamp from dual;

prompt building storet res_detect_qnt_lmt indexes
exec etl_helper_result_res_detect_qnt_lmt('storet');

select 'transform res_detect_qnt_lmt end time: ' || systimestamp from dual;
