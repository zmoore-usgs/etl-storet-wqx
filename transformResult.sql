show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform result start time: ' || systimestamp from dual;

prompt dropping storet pc_result indexes
exec etl_helper.drop_indexes('pc_result_swap_storet');

prompt populating storet pc_result
truncate table pc_result_swap_storet;

insert /*+ append parallel(4) */
  into pc_result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,
                              characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,
                              organization_name, activity_type_code, activity_media_subdiv_name, activity_start_time,
                              act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,
                              activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                              activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment,
                              sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,
                              result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,
                              result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                              temperature_basis_level, particle_size, precision, result_comment, result_depth_meas_value,
                              result_depth_meas_unit_code, result_depth_alt_ref_pt_txt, sample_tissue_taxonomic_name,
                              sample_tissue_anatomy_name, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                              analytical_method_citation, lab_name, analysis_date_time, lab_remark, detection_limit, detection_limit_unit,
                              detection_limit_desc, analysis_prep_date_tx)
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select activity.mloc_uid station_id, 
               station.site_id,
               trunc(activity.act_start_date) event_date,
	           case 
	             when analytical_method_.method_id is not null
	        	   then case analytical_method_.method_type
	        		      when 'analytical'
	        				then 'https://www.nemi.gov/methods/method_summary/' || analytical_method_.method_id || '/'
	        			  when 'statistical'
	        				then 'https://www.nemi.gov/methods/sams_method_summary/' || analytical_method_.method_id || '/'
	        			  end
	        	   else
	        		 null
	           end analytical_method,
               station.organization || '-' || activity.act_id activity,
               characteristic.chr_name characteristic_name,
               nvl(storetw_di_characteristic.characteristic_group_type, 'Not Assigned') characteristic_type,
               activity_media.acmed_name sample_media,
               station.organization,
               station.site_type,
               station.huc,
               station.governmental_unit_code,
               organization.org_name organization_name,              
               activity_type.actyp_cd activity_type_code,             
               activity_media_subdivision.amsub_name activity_media_subdiv_name,     
               nvl(to_char(activity.act_start_time, 'hh24:mi:ss'), '00:00:00') activity_start_time,            
               start_time_zone.tmzone_cd act_start_time_zone,            
               to_char(activity.act_end_date, 'yyyy-mm-dd') activity_stop_date,             
               nvl2(act_end_date, nvl(to_char(activity.act_end_time, 'hh24:mi:ss'), '00:00:00'), null) activity_stop_time,             
               end_time_zone.tmzone_cd act_stop_time_zone,             
               activity.act_depth_height activity_depth,                 
               h_measurement_unit.msunt_cd activity_depth_unit,            
               activity.act_depth_altitude_ref_point activity_depth_ref_point,       
               activity.act_depth_height_top activity_upper_depth,           
               t_measurement_unit.msunt_cd activity_upper_depth_unit,      
               activity.act_depth_height_bottom activity_lower_depth,           
               b_measurement_unit.msunt_cd activity_lower_depth_unit,      
               activity_project.project_id_list project_id,                     
               activity_conducting_org.acorg_name activity_conducting_org,       
               activity.act_comments activity_comment,             
               case
                 when activity.act_sam_collect_meth_id is not null and
                      activity.act_sam_collect_meth_context is not null
                   then activity.act_sam_collect_meth_id
                 else 'USEPA'
               end sample_collect_method_id,       
               case
                 when activity.act_sam_collect_meth_id is not null and
                      activity.act_sam_collect_meth_context is not null
                   then activity.act_sam_collect_meth_context
                 else 'USEPA'
               end sample_collect_method_ctx,      
               case
                 when activity.act_sam_collect_meth_id is not null and
                      activity.act_sam_collect_meth_context is not null
                   then activity.act_sam_collect_meth_name
                 else 'USEPA'
               end sample_collect_method_name,  
               nvl(sample_collection_equip.sceqp_name, 'Unknown') sample_collect_equip_name,      
               result.res_uid result_id,
               nvl(case when regexp_substr(result.res_measure, '^-?\d*\.?\d*$') is null then result.res_measure end, result_detection_condition.rdcnd_name) result_detection_condition_tx,
               sample_fraction.smfrc_name sample_fraction_type,
               case when regexp_substr(result.res_measure, '^-?\d*\.?\d*$') is not null then result.res_measure end result_measure_value,
               case when regexp_substr(result.res_measure, '^-?\d*\.?\d*$') is not null then rmeasurement_unit.msunt_cd end result_unit,
               result_measure_qualifier.rmqlf_cd result_meas_qual_code,
               result_status.ressta_name result_value_status,
               result_statistical_base.rsbas_cd statistic_type,
               result_value_type.rvtyp_name result_value_type,
               result_weight_basis.rwbas_name weight_basis_type,
               result_time_basis.rtimb_name duration_basis,
               result_temperature_basis.rtmpb_name temperature_basis_level,
               result.res_particle_size_basis particle_size,
               result.res_measure_precision precision,
               result.res_comments result_comment,
               result.res_depth_height result_depth_meas_value,
               dhmeasurement_unit.msunt_cd result_depth_meas_unit_code,
               result.res_depth_altitude_ref_point result_depth_alt_ref_pt_txt,
               taxon.tax_name sample_tissue_taxonomic_name,
               sample_tissue_anatomy.stant_name sample_tissue_anatomy_name,
         	   analytical_method_.anlmth_id analytical_procedure_id,
         	   analytical_method_.amctx_cd analytical_procedure_source,
        	   analytical_method_.anlmth_name analytical_method_name,
        	   analytical_method_.anlmth_url analytical_method_citation,
               result.res_lab_name lab_name,
               to_char(result.res_lab_analysis_start_date, 'yyyy-mm-dd') analysis_date_time,
               result_lab_comment.rlcom_desc lab_remark,
               detect.rdqlmt_measure detection_limit,
               detect.msunt_cd detection_limit_unit,
               detect.dqltyp_name detection_limit_desc,
               to_char(result_lab_sample_prep.rlsprp_start_date, 'yyyy-mm-dd') analysis_prep_date_tx
          from wqx.activity
               join wqx.result
                 on activity.act_uid = result.act_uid
               join station_swap_storet station
                 on activity.mloc_uid = station.station_id
               left join wqx.sample_collection_equip
                 on activity.sceqp_uid = sample_collection_equip.sceqp_uid
               left join wqx.activity_conducting_org
                 on activity.act_uid = activity_conducting_org.act_uid
               left join (select activity_project.act_uid,
                                 listagg(project.prj_id, ';') within group (order by project.prj_id) project_id_list
                            from wqx.activity_project
                                 left join wqx.project
                                   on activity_project.prj_uid = project.prj_uid
                               group by activity_project.act_uid) activity_project
                 on activity.act_uid = activity_project.act_uid
               left join wqx.measurement_unit b_measurement_unit
                 on activity.msunt_uid_depth_height_bottom = b_measurement_unit.msunt_uid
               left join wqx.measurement_unit t_measurement_unit
                 on activity.msunt_uid_depth_height_top = t_measurement_unit.msunt_uid
               left join wqx.measurement_unit h_measurement_unit
                 on activity.msunt_uid_depth_height = h_measurement_unit.msunt_uid
               left join wqx.time_zone end_time_zone
                 on activity.tmzone_uid_end_time = end_time_zone.tmzone_uid
               left join wqx.time_zone start_time_zone
                 on activity.tmzone_uid_start_time = start_time_zone.tmzone_uid
               left join wqx.activity_media_subdivision
                 on activity.amsub_uid = activity_media_subdivision.amsub_uid
               left join wqx.activity_type
                 on activity.actyp_uid = activity_type.actyp_uid
               left join wqx.organization
                 on activity.org_uid = organization.org_uid
               left join wqx.activity_media
                 on activity.acmed_uid = activity_media.acmed_uid
               left join wqx.characteristic
                 on result.chr_uid = characteristic.chr_uid
               left join wqx.result_detection_condition
                 on result.rdcnd_uid = result_detection_condition.rdcnd_uid
               left join wqx.sample_fraction
                 on result.smfrc_uid = sample_fraction.smfrc_uid
               left join wqx.measurement_unit rmeasurement_unit
                 on result.msunt_uid_measure = rmeasurement_unit.msunt_uid
               left join wqx.result_measure_qualifier
                 on result.rmqlf_uid = result_measure_qualifier.rmqlf_uid
               left join wqx.result_status
                 on result.ressta_uid = result_status.ressta_uid
               left join wqx.result_statistical_base
                 on result.rsbas_uid = result_statistical_base.rsbas_uid
               left join wqx.result_value_type
                 on result.rvtyp_uid = result_value_type.rvtyp_uid
               left join wqx.result_weight_basis
                 on result.rwbas_uid = result_weight_basis.rwbas_uid
               left join wqx.result_time_basis
                 on result.rtimb_uid = result_time_basis.rtimb_uid
               left join wqx.result_temperature_basis
                 on result.rtmpb_uid = result_temperature_basis.rtmpb_uid
               left join wqx.measurement_unit dhmeasurement_unit
                 on result.msunt_uid_depth_height = dhmeasurement_unit.msunt_uid
               left join (select analytical_method.anlmth_uid,
                                 analytical_method.anlmth_id,
                                 analytical_method_context.amctx_cd,
                                 analytical_method.anlmth_name,
                                 analytical_method.anlmth_url,
                                 wqp_nemi_epa_crosswalk.method_id,
                                 wqp_nemi_epa_crosswalk.method_type
                            from wqx.analytical_method
                                 left join wqx.analytical_method_context
                                   on analytical_method.amctx_uid = analytical_method_context.amctx_uid
                                 left join wqp_nemi_epa_crosswalk
                                   on analytical_method_context.amctx_cd = wqp_nemi_epa_crosswalk.analytical_procedure_source and
                                      analytical_method.anlmth_id = wqp_nemi_epa_crosswalk.analytical_procedure_id ) analytical_method_ 
                 on result.anlmth_uid = analytical_method_.anlmth_uid
               left join (select result_detect_quant_limit.res_uid,
                                 result_detect_quant_limit.rdqlmt_measure,
                                 measurement_unit.msunt_cd,
                                 detection_quant_limit_type.dqltyp_name
                            from wqx.result_detect_quant_limit
                                 left join wqx.measurement_unit
                                   on result_detect_quant_limit.msunt_uid = measurement_unit.msunt_uid
                                 left join wqx.detection_quant_limit_type
                                   on result_detect_quant_limit.dqltyp_uid = detection_quant_limit_type.dqltyp_uid) detect
                 on result.res_uid = detect.res_uid
               left join wqx.result_lab_sample_prep
                 on result.res_uid = result_lab_sample_prep.res_uid 
               left join wqx.taxon
                 on result.tax_uid = taxon.tax_uid
               left join wqx.sample_tissue_anatomy
                 on result.stant_uid = sample_tissue_anatomy.stant_uid
               left join wqx.result_lab_comment
                 on result.rlcom_uid = result_lab_comment.rlcom_uid
               left join storetw_di_characteristic
                 on characteristic.chr_storet_id = storetw_di_characteristic.pk_isn
         where activity.acmed_uid <> 3) a
    order by a.station_id;

commit;

insert /*+ append parallel(4) */
  into pc_result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,
                              characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,
                              organization_name, activity_type_code, activity_media_subdiv_name, activity_start_time,
                              act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,
                              activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                              activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment,
                              sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,
                              result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,
                              result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                              temperature_basis_level, particle_size, precision, result_comment, result_depth_meas_value,
                              result_depth_meas_unit_code, result_depth_alt_ref_pt_txt, sample_tissue_taxonomic_name,
                              sample_tissue_anatomy_name, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                              analytical_method_citation, lab_name, analysis_date_time, lab_remark, detection_limit, detection_limit_unit,
                              detection_limit_desc, analysis_prep_date_tx)
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select fa_result_no_source.station_id, 
               station.site_id,
               fa_result_no_source.event_date,
	           fa_result_no_source.analytical_method,
               fa_result_no_source.activity,
               fa_result_no_source.characteristic_name,
               fa_result_no_source.characteristic_type,
               fa_result_no_source.sample_media,
               station.organization,
               station.site_type,
               station.huc,
               station.governmental_unit_code,
               station.organization_name,              
               fa_result_no_source.activity_type_code,             
               fa_result_no_source.activity_media_subdiv_name,     
               fa_result_no_source.activity_start_time,            
               fa_result_no_source.act_start_time_zone,            
               fa_result_no_source.activity_stop_date,             
               fa_result_no_source.activity_stop_time,             
               fa_result_no_source.act_stop_time_zone,             
               fa_result_no_source.activity_depth,                 
               fa_result_no_source.activity_depth_unit,            
               fa_result_no_source.activity_depth_ref_point,       
               fa_result_no_source.activity_upper_depth,           
               fa_result_no_source.activity_upper_depth_unit,      
               fa_result_no_source.activity_lower_depth,           
               fa_result_no_source.activity_lower_depth_unit,      
               fa_result_no_source.project_id,                     
               fa_result_no_source.activity_conducting_org,       
               fa_result_no_source.activity_comment,             
               fa_result_no_source.sample_collect_method_id,       
               fa_result_no_source.sample_collect_method_ctx,      
               fa_result_no_source.sample_collect_method_name,  
               fa_result_no_source.sample_collect_equip_name,      
               fa_result_no_source.result_id,
               fa_result_no_source.result_detection_condition_tx,
               fa_result_no_source.sample_fraction_type,
               fa_result_no_source.result_measure_value,
               fa_result_no_source.result_unit,
               fa_result_no_source.result_meas_qual_code,
               fa_result_no_source.result_value_status,
               fa_result_no_source.statistic_type,
               fa_result_no_source.result_value_type,
               fa_result_no_source.weight_basis_type,
               fa_result_no_source.duration_basis,
               fa_result_no_source.temperature_basis_level,
               fa_result_no_source.particle_size,
               fa_result_no_source.precision,
               fa_result_no_source.result_comment,
               fa_result_no_source.result_depth_meas_value,
               fa_result_no_source.result_depth_meas_unit_code,
               fa_result_no_source.result_depth_alt_ref_pt_txt,
               fa_result_no_source.sample_tissue_taxonomic_name,
               null sample_tissue_anatomy_name,
         	   fa_result_no_source.analytical_procedure_id,
         	   fa_result_no_source.analytical_procedure_source,
        	   fa_result_no_source.analytical_method_name,
        	   null analytical_method_citation,
               fa_result_no_source.lab_name,
               fa_result_no_source.analysis_date_time,
               fa_result_no_source.lab_remark,
               fa_result_no_source.detection_limit,
               fa_result_no_source.detection_limit_unit,
               fa_result_no_source.detection_limit_desc,
               fa_result_no_source.analysis_prep_date_tx
          from fa_result_no_source
               join station_swap_storet station
                 on fa_result_no_source.station_id + 10000000 = station.station_id) a
    order by a.station_id;

commit;

select 'transform result end time: ' || systimestamp from dual;
