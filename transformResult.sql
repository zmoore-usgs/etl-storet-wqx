show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform result start time: ' || systimestamp from dual;

prompt populating wqp_nemi_epa_crosswalk
truncate table wqp_nemi_epa_crosswalk;
insert /*+ append parallel(4) */ into wqp_nemi_epa_crosswalk
select /*+ parallel(4) */ 
       wqp_source,
       analytical_procedure_source,
       analytical_procedure_id,
       source_method_identifier,
       method_id,
       method_source,
       method_type,
       case
         when method_id is not null
           then
             case method_type
               when 'analytical'
                 then 'https://www.nemi.gov/methods/method_summary/' || method_id || '/'
               when 'statistical'
                 then 'https://www.nemi.gov/methods/sams_method_summary/' || method_id || '/'
               end
         else 
           null 
       end
  from (select wqp_nemi_epa_crosswalk.*,
               count(*) over (partition by analytical_procedure_source, analytical_procedure_id) cnt
          from wqp_nemi_epa_crosswalk@nemi.er.usgs.gov)
 where cnt = 1;
commit;
select 'Building wqp_nemi_epa_crosswalk complete: ' || systimestamp from dual;

prompt populating wqx_activity_project
truncate table wqx_activity_project;
insert /*+ append parallel(4) */ into wqx_activity_project (act_uid, project_id_list)
select /*+ parallel(4) */ 
       activity_project.act_uid,
       listagg(project.prj_id, ';') within group (order by project.prj_id) project_id_list
  from wqx.activity_project
       left join wqx.project
         on activity_project.prj_uid = project.prj_uid
    group by activity_project.act_uid;
commit;
select 'Building wqx_actvity_project complete: ' || systimestamp from dual;

prompt populating wqx_analytical_method
truncate table wqx_analytical_method;
insert /*+ append parallel(4) */ into wqx_analytical_method (anlmth_uid, anlmth_id, amctx_cd, anlmth_name, anlmth_url, anlmth_qual_type, nemi_url)
select /*+ parallel(4) */ 
       analytical_method.anlmth_uid,
       analytical_method.anlmth_id,
       analytical_method_context.amctx_cd,
       analytical_method.anlmth_name,
       analytical_method.anlmth_url,
       analytical_method.anlmth_qual_type,
       wqp_nemi_epa_crosswalk.nemi_url
  from wqx.analytical_method
       left join wqx.analytical_method_context
         on analytical_method.amctx_uid = analytical_method_context.amctx_uid
       left join wqp_nemi_epa_crosswalk
         on analytical_method_context.amctx_cd = wqp_nemi_epa_crosswalk.analytical_procedure_source and
            analytical_method.anlmth_id = wqp_nemi_epa_crosswalk.analytical_procedure_id;
commit;
select 'Building wqx_analytical_method complete: ' || systimestamp from dual;

prompt populating wqx_detection_quant_limit
truncate table wqx_detection_quant_limit;
insert /*+ append parallel(4) */ into wqx_detection_quant_limit (res_uid, rdqlmt_measure, msunt_cd, dqltyp_name)
select /*+ parallel(4) */ res_uid, rdqlmt_measure, msunt_cd, dqltyp_name
  from (select result_detect_quant_limit.res_uid,
               result_detect_quant_limit.rdqlmt_measure,
               measurement_unit.msunt_cd,
               detection_quant_limit_type.dqltyp_name,
               count(*) over (partition by res_uid, result_detect_quant_limit.dqltyp_uid) dup_cnt,
               dense_rank() over (partition by res_uid order by case when result_detect_quant_limit.dqltyp_uid = 2 then 1 else 99 end) my_rank
          from wqx.result_detect_quant_limit
               left join wqx.measurement_unit
                 on result_detect_quant_limit.msunt_uid = measurement_unit.msunt_uid
               left join wqx.detection_quant_limit_type
                 on result_detect_quant_limit.dqltyp_uid = detection_quant_limit_type.dqltyp_uid
         where result_detect_quant_limit.dqltyp_uid not in (4,5,8,9,10))
 where dup_cnt = 1 and
       my_rank = 1;
commit;
select 'Building wqx_detection_quant_limit complete: ' || systimestamp from dual;



prompt populating wqx_activity
truncate table wqx_activity;
insert /*+ append parallel(4) */
  into wqx_activity (act_uid, station_id, site_id, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,
                     organization_name, activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone,
                     activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name, activity_depth,
                     activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit, activity_lower_depth,
                     activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment, activity_latitude, activity_longitude,
                     activity_source_map_scale, act_horizontal_accuracy, act_horizontal_accuracy_unit, act_horizontal_collect_method,
                     act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration, act_collection_duration_unit,
                     act_sam_compnt_name, act_sam_compnt_place_in_series, act_reach_length, act_reach_length_unit, act_reach_width,
                     act_reach_width_unit, act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size,
                     act_net_mesh_size_unit, act_boat_speed, act_boat_speed_unit, act_current_speed, act_current_speed_unit,
                     toxicity_test_type_name, sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,
                     act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments,
                     act_sam_prep_meth_id, act_sam_prep_meth_context, act_sam_prep_meth_name, act_sam_prep_meth_qual_type,
                     act_sam_prep_meth_desc, sample_container_type, sample_container_color, act_sam_chemical_preservative,
                     thermal_preservative_name, act_sam_transport_storage_desc)
select /*+ parallel(4) */ 
       activity.act_uid,
               activity.mloc_uid station_id, 
               station.site_id,
               trunc(activity.act_start_date) event_date,
               station.organization || '-' || activity.act_id activity,
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
               relative_depth.reldpth_name activity_relative_depth_name,
               activity.act_depth_height activity_depth,                 
               h_measurement_unit.msunt_cd activity_depth_unit,            
               activity.act_depth_altitude_ref_point activity_depth_ref_point,       
               activity.act_depth_height_top activity_upper_depth,           
               t_measurement_unit.msunt_cd activity_upper_depth_unit,      
               activity.act_depth_height_bottom activity_lower_depth,           
               b_measurement_unit.msunt_cd activity_lower_depth_unit,      
               wqx_activity_project.project_id_list project_id,                     
               activity_conducting_org.acorg_name_list activity_conducting_org,       
               activity.act_comments activity_comment,    
               activity.act_loc_latitude activity_latitude,
               activity.act_loc_longitude activity_longitude,
               activity.act_loc_source_map_scale activity_source_map_scale,
               activity.act_horizontal_accuracy,
               activity_horizontal_unit.msunt_cd act_horizontal_accuracy_unit,
               horizontal_collection_method.hcmth_name act_horizontal_collect_method,
               horizontal_reference_datum.hrdat_name act_horizontal_datum_name,
               assemblage.asmblg_name assemblage_sampled_name,
               activity.act_collection_duration,
               collection_duration.msunt_cd act_collection_duration_unit,
               activity.act_sam_compnt_name,
               activity.act_sam_compnt_place_in_series,
               activity.act_reach_length,
               reach_length.msunt_cd act_reach_length_unit,
               activity.act_reach_width,
               reach_width.msunt_cd act_reach_width_unit,
               activity.act_pass_count,
               net_type.nettyp_name net_type_name,
               activity.act_net_surface_area,
               net_surface_unit.msunt_cd act_net_surface_area_unit,
               activity.act_net_mesh_size,
               net_mesh.msunt_cd act_net_mesh_size_unit,
               activity.act_boat_speed,
               boat_speed.msunt_cd act_boat_speed_unit,
               activity.act_current_speed,
               current_speed.msunt_cd act_current_speed_unit,
               toxicity_test_type.tttyp_name toxicity_test_type_name,
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
               activity.act_sam_collect_meth_qual_type,
               activity.act_sam_collect_meth_desc,
               nvl(sample_collection_equip.sceqp_name, 'Unknown') sample_collect_equip_name,
               activity.act_sam_collect_equip_comments,
               activity.act_sam_prep_meth_id,
               activity.act_sam_prep_meth_context,
               activity.act_sam_prep_meth_name,
               activity.act_sam_prep_meth_qual_type,
               activity.act_sam_prep_meth_desc,
               container_type.contyp_name sample_container_type,
               container_color.concol_name sample_container_color,
               activity.act_sam_chemical_preservative,
               thermal_preservative.thprsv_name thermal_preservative_name,
               activity.act_sam_transport_storage_desc
          from wqx.activity
               join station_swap_storet station
                 on activity.mloc_uid = station.station_id
               left join wqx.sample_collection_equip
                 on activity.sceqp_uid = sample_collection_equip.sceqp_uid
               left join (select act_uid,
                                 listagg(acorg_name, ';') within group (order by rownum) acorg_name_list
                            from wqx.activity_conducting_org
                              group by act_uid) activity_conducting_org
                 on activity.act_uid = activity_conducting_org.act_uid
               left join wqx_activity_project
                 on activity.act_uid = wqx_activity_project.act_uid
               left join wqx.measurement_unit b_measurement_unit
                 on activity.msunt_uid_depth_height_bottom = b_measurement_unit.msunt_uid
               left join wqx.measurement_unit t_measurement_unit
                 on activity.msunt_uid_depth_height_top = t_measurement_unit.msunt_uid
               left join wqx.measurement_unit h_measurement_unit
                 on activity.msunt_uid_depth_height = h_measurement_unit.msunt_uid
               left join wqx.measurement_unit net_surface_unit
                 on activity.msunt_uid_net_surface_area = net_surface_unit.msunt_uid
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
               left join wqx.measurement_unit activity_horizontal_unit
                 on activity.msunt_uid_horizontal_accuracy = activity_horizontal_unit.msunt_uid
               left join wqx.horizontal_collection_method
                 on activity.hcmth_uid = horizontal_collection_method.hcmth_uid
               left join wqx.horizontal_reference_datum
                 on activity.hrdat_uid = horizontal_reference_datum.hrdat_uid
               left join wqx.assemblage
                 on activity.asmblg_uid = assemblage.asmblg_uid 
               left join wqx.measurement_unit collection_duration	 
                 on activity.msunt_uid_collection_duration = collection_duration.msunt_uid
               left join wqx.measurement_unit reach_length
                 on activity.msunt_uid_reach_length = reach_length.msunt_uid
               left join wqx.measurement_unit reach_width
                 on activity.msunt_uid_reach_width = reach_width.msunt_uid
               left join wqx.net_type
                 on activity.nettyp_uid = net_type.nettyp_uid
               left join wqx.measurement_unit net_mesh
                 on activity.msunt_uid_net_mesh_size = net_mesh.msunt_uid
               left join wqx.measurement_unit boat_speed
                 on activity.msunt_uid_boat_speed = boat_speed.msunt_uid
               left join wqx.measurement_unit current_speed
                 on activity.msunt_uid_current_speed = current_speed.msunt_uid
               left join wqx.toxicity_test_type
                 on activity.tttyp_uid = toxicity_test_type.tttyp_uid
               left join wqx.container_type
                 on activity.contyp_uid = container_type.contyp_uid
               left join wqx.container_color
                 on activity.concol_uid = container_color.concol_uid
               left join wqx.thermal_preservative
                 on activity.thprsv_uid = thermal_preservative.thprsv_uid
               left join wqx.relative_depth
                 on activity.reldpth_uid = relative_depth.reldpth_uid;
commit;
select 'Building wqx_activity complete: ' || systimestamp from dual;


prompt dropping storet result indexes
exec etl_helper_result.drop_indexes('storet');

prompt populating storet result
truncate table result_swap_storet;

insert /*+ append parallel(4) */
  into result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,
                           characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,
                           organization_name, activity_type_code, activity_media_subdiv_name, activity_start_time,
                           act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name, activity_depth,
                           activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                           activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment,
                           activity_latitude, activity_longitude, activity_source_map_scale, act_horizontal_accuracy, act_horizontal_accuracy_unit,
                           act_horizontal_collect_method, act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration, act_collection_duration_unit,
                           act_sam_compnt_name, act_sam_compnt_place_in_series, act_reach_length, act_reach_length_unit, act_reach_width, act_reach_width_unit,
                           act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size, act_net_mesh_size_unit, act_boat_speed,
                           act_boat_speed_unit, act_current_speed, act_current_speed_unit, toxicity_test_type_name,
                           sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,
                           act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments, act_sam_prep_meth_id,
                           act_sam_prep_meth_context, act_sam_prep_meth_name, act_sam_prep_meth_qual_type, act_sam_prep_meth_desc, sample_container_type,
                           sample_container_color, act_sam_chemical_preservative, thermal_preservative_name, act_sam_transport_storage_desc,
                           result_id, res_data_logger_line, result_detection_condition_tx, method_specification_name, sample_fraction_type, result_measure_value,
                           result_unit, result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                           temperature_basis_level, particle_size, precision, res_measure_bias, res_measure_conf_interval, res_measure_upper_conf_limit,
                           res_measure_lower_conf_limit, result_comment, result_depth_meas_value, result_depth_meas_unit_code, result_depth_alt_ref_pt_txt,
                           res_sampling_point_name, biological_intent, res_bio_individual_id, sample_tissue_taxonomic_name, unidentifiedspeciesidentifier,
                           sample_tissue_anatomy_name, res_group_summary_ct_wt, res_group_summary_ct_wt_unit, cell_form_name, cell_shape_name, habit_name, volt_name,
                           rtdet_pollution_tolerance, rtdet_pollution_tolernce_scale, rtdet_trophic_level, rtfgrp_functional_feeding_grp, taxon_citation_title,
                           taxon_citation_creator, taxon_citation_subject, taxon_citation_publisher, taxon_citation_date, taxon_citation_id, fcdsc_name,
                           frequency_class_unit, fcdsc_lower_bound, fcdsc_upper_bound, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                           anlmth_qual_type, analytical_method_citation, lab_name, analysis_start_date, analysis_start_time, analysis_start_timezone, analysis_end_date,
                           analysis_end_time, analysis_end_timezone, rlcom_cd, lab_remark, detection_limit, detection_limit_unit, detection_limit_desc,
                           res_lab_accred_yn, res_lab_accred_authority, res_taxonomist_accred_yn, res_taxonomist_accred_authorty, prep_method_id, prep_method_context,
                           prep_method_name, prep_method_qual_type, prep_method_desc, analysis_prep_date_tx, prep_start_time, prep_start_timezone, prep_end_date,
                           prep_end_time, prep_end_timezone, prep_dilution_factor)
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select /*+ parallel(4) */ 
               wqx_activity.station_id, 
               wqx_activity.site_id,
               wqx_activity.event_date,
               wqx_analytical_method.nemi_url analytical_method,
               wqx_activity.activity,
               characteristic.chr_name characteristic_name,
               nvl(di_characteristic.characteristic_group_type, 'Not Assigned') characteristic_type,
               wqx_activity.sample_media,
               wqx_activity.organization,
               wqx_activity.site_type,
               wqx_activity.huc,
               wqx_activity.governmental_unit_code,
               wqx_activity.organization_name,
               wqx_activity.activity_type_code,
               wqx_activity.activity_media_subdiv_name,
               wqx_activity.activity_start_time,
               wqx_activity.act_start_time_zone,
               wqx_activity.activity_stop_date,
               wqx_activity.activity_stop_time,
               wqx_activity.act_stop_time_zone,
               wqx_activity.activity_relative_depth_name,
               wqx_activity.activity_depth,
               wqx_activity.activity_depth_unit,
               wqx_activity.activity_depth_ref_point,
               wqx_activity.activity_upper_depth,
               wqx_activity.activity_upper_depth_unit,
               wqx_activity.activity_lower_depth,
               wqx_activity.activity_lower_depth_unit,
               wqx_activity.project_id,
               wqx_activity.activity_conducting_org,
               wqx_activity.activity_comment,
               wqx_activity.activity_latitude,
               wqx_activity.activity_longitude,
               wqx_activity.activity_source_map_scale,
               wqx_activity.act_horizontal_accuracy,
               wqx_activity.act_horizontal_accuracy_unit,
               wqx_activity.act_horizontal_collect_method,
               wqx_activity.act_horizontal_datum_name,
               wqx_activity.assemblage_sampled_name,
               wqx_activity.act_collection_duration,
               wqx_activity.act_collection_duration_unit,
               wqx_activity.act_sam_compnt_name,
               wqx_activity.act_sam_compnt_place_in_series,
               wqx_activity.act_reach_length,
               wqx_activity.act_reach_length_unit,
               wqx_activity.act_reach_width,
               wqx_activity.act_reach_width_unit,
               wqx_activity.act_pass_count,
               wqx_activity.net_type_name,
               wqx_activity.act_net_surface_area,
               wqx_activity.act_net_surface_area_unit,
               wqx_activity.act_net_mesh_size,
               wqx_activity.act_net_mesh_size_unit,
               wqx_activity.act_boat_speed,
               wqx_activity.act_boat_speed_unit,
               wqx_activity.act_current_speed,
               wqx_activity.act_current_speed_unit,
               wqx_activity.toxicity_test_type_name,
               wqx_activity.sample_collect_method_id,
               wqx_activity.sample_collect_method_ctx,
               wqx_activity.sample_collect_method_name,
               wqx_activity.act_sam_collect_meth_qual_type,
               wqx_activity.act_sam_collect_meth_desc,
               wqx_activity.sample_collect_equip_name,
               wqx_activity.act_sam_collect_equip_comments,
               wqx_activity.act_sam_prep_meth_id,
               wqx_activity.act_sam_prep_meth_context,
               wqx_activity.act_sam_prep_meth_name,
               wqx_activity.act_sam_prep_meth_qual_type,
               wqx_activity.act_sam_prep_meth_desc,
               wqx_activity.sample_container_type,
               wqx_activity.sample_container_color,
               wqx_activity.act_sam_chemical_preservative,
               wqx_activity.thermal_preservative_name,
               wqx_activity.act_sam_transport_storage_desc,
               result.res_uid result_id,
               result.res_data_logger_line,
               result_detection_condition.rdcnd_name result_detection_condition_tx,
               method_speciation.mthspc_name method_specification_name,
               sample_fraction.smfrc_name sample_fraction_type,
               result.res_measure result_measure_value,
               rmeasurement_unit.msunt_cd result_unit,
               result_measure_qualifier.rmqlf_cd result_meas_qual_code,
               result_status.ressta_name result_value_status,
               result_statistical_base.rsbas_cd statistic_type,
               result_value_type.rvtyp_name result_value_type,
               result_weight_basis.rwbas_name weight_basis_type,
               result_time_basis.rtimb_name duration_basis,
               result_temperature_basis.rtmpb_name temperature_basis_level,
               result.res_particle_size_basis particle_size,
               result.res_measure_precision precision,
               result.res_measure_bias,
               result.res_measure_conf_interval,
               result.res_measure_upper_conf_limit,
               result.res_measure_lower_conf_limit,
               result.res_comments result_comment,
               result.res_depth_height result_depth_meas_value,
               dhmeasurement_unit.msunt_cd result_depth_meas_unit_code,
               result.res_depth_altitude_ref_point result_depth_alt_ref_pt_txt,
               result.res_sampling_point_name,
               biological_intent.bioint_name biological_intent,
               result.res_bio_individual_id,
               taxon.tax_name sample_tissue_taxonomic_name,
               result.res_species_id UnidentifiedSpeciesIdentifier,
               sample_tissue_anatomy.stant_name sample_tissue_anatomy_name,
               result.res_group_summary_ct_wt,
               group_summ_ct_wt.msunt_cd res_group_summary_ct_wt_unit,
               cell_form.celfrm_name cell_form_name,
               cell_shape.celshp_name cell_shape_name,
               habit.habit_name_list habit_name,
               voltinism.volt_name,
               result_taxon_detail.rtdet_pollution_tolerance,
               result_taxon_detail.rtdet_pollution_tolernce_scale,
               result_taxon_detail.rtdet_trophic_level,
               result_taxon_feeding_group.feeding_group_list rtfgrp_functional_feeding_grp,
               taxon_citation.citatn_title,
               taxon_citation.citatn_creator,
               taxon_citation.citatn_subject,
               taxon_citation.citatn_publisher,
               taxon_citation.citatn_date,
               taxon_citation.citatn_id,
               /*frequency_class_descriptor.*/ null fcdsc_name,
               /*result_frequency.msunt_cd*/ null frequency_class_unit,
               /*result_frequency_class.*/ null fcdsc_lower_bound,
               /*result_frequency_class.*/ null fcdsc_upper_bound,
               wqx_analytical_method.anlmth_id analytical_procedure_id,
               wqx_analytical_method.amctx_cd analytical_procedure_source,
               wqx_analytical_method.anlmth_name analytical_method_name,
               wqx_analytical_method.anlmth_qual_type,
               wqx_analytical_method.anlmth_url analytical_method_citation,
               result.res_lab_name lab_name,
               to_char(result.res_lab_analysis_start_date, 'yyyy-mm-dd') analysis_start_date,
               result.res_lab_analysis_start_time,
               analysis_start.tmzone_cd analysis_start_timezone,
               result.res_lab_analysis_end_date,
               result.res_lab_analysis_end_time,
               analysis_end.tmzone_cd analysis_end_timezone,
               result_lab_comment.rlcom_cd,
               result_lab_comment.rlcom_desc lab_remark,
               wqx_detection_quant_limit.rdqlmt_measure detection_limit,
               wqx_detection_quant_limit.msunt_cd detection_limit_unit,
               wqx_detection_quant_limit.dqltyp_name detection_limit_desc,
               result.res_lab_accred_yn,
               result.res_lab_accred_authority,
               result.res_taxonomist_accred_yn,
               result.res_taxonomist_accred_authorty,
               /*result_lab_sample_prep.*/ null rlsprp_method_id,
               /*result_lab_sample_prep.*/ null rlsprp_method_context,
               /*result_lab_sample_prep.*/ null rlsprp_method_name,
               /*result_lab_sample_prep.*/ null rlsprp_method_qual_type,
               /*result_lab_sample_prep.*/ null rlsprp_method_desc,
               /*to_char(result_lab_sample_prep.rlsprp_start_date, 'yyyy-mm-dd')*/ null analysis_prep_date_tx,
               /*result_lab_sample_prep.*/ null rlsprp_start_time,
               /*prep_start.tmzone_cd*/ null prep_start_timezone,
               /*result_lab_sample_prep.*/ null rlsprp_end_date,
               /*result_lab_sample_prep.*/ null rlsprp_end_time,
               /*prep_end.tmzone_cd*/ null prep_end_time,
               /*result_lab_sample_prep.*/ null rlsprp_dilution_factor
          from wqx_activity
               join wqx.result
                 on wqx_activity.act_uid = result.act_uid
               left join wqx.method_speciation
                 on result.mthspc_uid = method_speciation.mthspc_uid
               left join wqx.biological_intent
                 on result.bioint_uid = biological_intent.bioint_uid
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
               left join wqx.measurement_unit group_summ_ct_wt
                 on result.msunt_uid_group_summary_ct_wt = group_summ_ct_wt.msunt_uid  
               left join wqx_analytical_method 
                 on result.anlmth_uid = wqx_analytical_method.anlmth_uid
               left join wqx_detection_quant_limit
                 on result.res_uid = wqx_detection_quant_limit.res_uid
/*             left join wqx.result_lab_sample_prep
                 on result.res_uid = result_lab_sample_prep.res_uid
               left join wqx.time_zone prep_start
                 on result_lab_sample_prep.tmzone_uid_start_time = prep_start.tmzone_uid
               left join wqx.time_zone prep_end
                 on result_lab_sample_prep.tmzone_uid_end_time = prep_end.tmzone_uid */
               left join wqx.time_zone analysis_start
                 on result.tmzone_uid_lab_analysis_start = analysis_start.tmzone_uid 
               left join wqx.time_zone analysis_end
                 on result.tmzone_uid_lab_analysis_end = analysis_end.tmzone_uid 
               left join wqx.taxon
                 on result.tax_uid = taxon.tax_uid
               left join wqx.sample_tissue_anatomy
                 on result.stant_uid = sample_tissue_anatomy.stant_uid
               left join wqx.result_lab_comment
                 on result.rlcom_uid = result_lab_comment.rlcom_uid
               left join storetw.di_characteristic
                 on characteristic.chr_storet_id = di_characteristic.pk_isn
               left join (select result_taxon_habit.res_uid,
                                 listagg(habit.habit_name, ';') within group (order by habit.habit_uid) habit_name_list
                            from wqx.result_taxon_habit
                                 left join wqx.habit
                                   on result_taxon_habit.habit_uid = habit.habit_uid
                               group by result_taxon_habit.res_uid) habit
                 on result.res_uid = habit.res_uid
               left join wqx.result_taxon_detail
                 on result.res_uid = result_taxon_detail.res_uid
               left join wqx.voltinism
                 on result_taxon_detail.volt_uid = voltinism.volt_uid
               left join (select res_uid,
                                 listagg(rtfgrp_functional_feeding_grp, ';') within group (order by rownum) feeding_group_list
                            from wqx.result_taxon_feeding_group
                              group by res_uid) result_taxon_feeding_group
                 on result.res_uid = result_taxon_feeding_group.res_uid
               left join wqx.citation taxon_citation
                 on result_taxon_detail.citatn_uid = taxon_citation.citatn_uid
               left join wqx.cell_form
                 on result_taxon_detail.celfrm_uid = cell_form.celfrm_uid
               left join wqx.cell_shape
                 on result_taxon_detail.celshp_uid = cell_shape.celshp_uid
/*               left join wqx.result_frequency_class
                 on result.res_uid = result_frequency_class.res_uid
               left join wqx.frequency_class_descriptor
                 on result_frequency_class.fcdsc_uid = frequency_class_descriptor.fcdsc_uid
               left join wqx.measurement_unit result_frequency
                 on result_frequency_class.msunt_uid = result_frequency.msunt_uid */
         where result.ressta_uid != 5
        ) a;
--    order by a.station_id;
commit;
select 'Building result_swap_storet from wqx complete: ' || systimestamp from dual;

insert /*+ append parallel(4) */
  into result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,
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
                           analytical_procedure_id, analytical_procedure_source, analytical_method_name, lab_name,
                           analysis_start_date, lab_remark, detection_limit, detection_limit_unit, detection_limit_desc, analysis_prep_date_tx)
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select /*+ parallel(4) */ 
               station.station_id, 
               station.site_id,
               result_no_source.event_date,
               result_no_source.analytical_method,
               result_no_source.activity,
               result_no_source.characteristic_name,
               result_no_source.characteristic_type,
               result_no_source.sample_media,
               station.organization,
               station.site_type,
               station.huc,
               station.governmental_unit_code,
               station.organization_name,
               result_no_source.activity_type_code,
               result_no_source.activity_media_subdiv_name,
               result_no_source.activity_start_time,
               result_no_source.act_start_time_zone,
               result_no_source.activity_stop_date,
               result_no_source.activity_stop_time,
               result_no_source.act_stop_time_zone,
               result_no_source.activity_depth,
               result_no_source.activity_depth_unit,
               result_no_source.activity_depth_ref_point,
               result_no_source.activity_upper_depth,
               result_no_source.activity_upper_depth_unit,
               result_no_source.activity_lower_depth,
               result_no_source.activity_lower_depth_unit,
               result_no_source.project_id,
               result_no_source.activity_conducting_org,
               result_no_source.activity_comment,
               result_no_source.sample_collect_method_id,
               result_no_source.sample_collect_method_ctx,
               result_no_source.sample_collect_method_name,
               result_no_source.sample_collect_equip_name,
               result_no_source.result_id,
               result_no_source.result_detection_condition_tx,
               result_no_source.sample_fraction_type,
               result_no_source.result_measure_value,
               result_no_source.result_unit,
               result_no_source.result_meas_qual_code,
               result_no_source.result_value_status,
               result_no_source.statistic_type,
               result_no_source.result_value_type,
               result_no_source.weight_basis_type,
               result_no_source.duration_basis,
               result_no_source.temperature_basis_level,
               result_no_source.particle_size,
               result_no_source.precision,
               result_no_source.result_comment,
               result_no_source.result_depth_meas_value,
               result_no_source.result_depth_meas_unit_code,
               result_no_source.result_depth_alt_ref_pt_txt,
               result_no_source.sample_tissue_taxonomic_name,
               result_no_source.analytical_procedure_id,
               result_no_source.analytical_procedure_source,
               result_no_source.analytical_method_name,
               result_no_source.lab_name,
               result_no_source.analysis_date_time,
               result_no_source.lab_remark,
               result_no_source.detection_limit,
               result_no_source.detection_limit_unit,
               result_no_source.detection_limit_desc,
               result_no_source.analysis_prep_date_tx
          from result_no_source
               join station_swap_storet station
                 on result_no_source.station_id + 10000000 = station.station_id) a;
--    order by a.station_id;
commit;
select 'Building result_swap_storet from result_no_source complete: ' || systimestamp from dual;

prompt building storet result indexes
exec etl_helper_result.create_indexes('storet');

select 'transform result end time: ' || systimestamp from dual;
