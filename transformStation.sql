show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform station start time: ' || systimestamp from dual;

prompt updating temporary table wqx_station_local from wqx
delete from wqx_station_local
 where station_source = 'WQX' and
       not exists (select null
                     from wqx.monitoring_location
                    where wqx_station_local.station_id = monitoring_location.mloc_uid);
commit;
merge into wqx_station_local o 
      using (select 'WQX' station_source,
                    monitoring_location.mloc_uid station_id,
                    org.org_id || '-' || monitoring_location.mloc_id site_id,
                    monitoring_location.mloc_latitude latitude,
                    monitoring_location.mloc_longitude longitude,
                    monitoring_location.hrdat_uid hrdat_uid,
                    nvl(mloc_huc_12, mloc_huc_8) huc,
                    nvl(country.cntry_cd,country_from_state.cntry_cd) cntry_cd,
                    to_char(state.st_fips_cd, 'fm00') st_fips_cd,
                    county.cnty_fips_cd,
                    sdo_cs.transform(mdsys.sdo_geometry(2001,
                                                        wqx_hrdat_to_srid.srid,
                                                        mdsys.sdo_point_type(round(monitoring_location.mloc_longitude, 7),
                                                                             round(monitoring_location.mloc_latitude, 7),
                                                                             null),
                                                        null, null),
                                     4269) geom
               from wqx.monitoring_location
                    join wqx_hrdat_to_srid
                      on monitoring_location.hrdat_uid = wqx_hrdat_to_srid.hrdat_uid
                    left join wqx.organization org
                      on monitoring_location.org_uid = org.org_uid
                    left join wqx.country
                      on monitoring_location.cntry_uid = country.cntry_uid
                    left join wqx.state
                      on monitoring_location.st_uid = state.st_uid
                    left join wqx.county
                      on monitoring_location.cnty_uid = county.cnty_uid
                    left join wqx.country country_from_state
                      on state.cntry_uid = country_from_state.cntry_uid
              where org.org_id not like '%TEST%' and
                    org.org_id not like '%TRAINING%'
            ) n
  on (o.station_source = n.station_source and
      o.station_id = n.station_id)
when matched then update
                     set o.site_id = n.site_id,
                         o.latitude = n.latitude,
                         o.longitude = n.longitude,
                         o.hrdat_uid = n.hrdat_uid,
                         o.huc = n.huc,
                         o.cntry_cd = n.cntry_cd,
                         o.st_fips_cd = n.st_fips_cd,
                         o.cnty_fips_cd = n.cnty_fips_cd,
                         o.calculated_huc_12 = null,
                         o.calculated_fips = null,
                         o.geom = n.geom
                   where lnnvl(o.latitude = n.latitude) or
                         lnnvl(o.longitude = n.longitude) or
                         lnnvl(o.hrdat_uid = n.hrdat_uid) or
                         lnnvl(o.huc = n.huc) or
                         lnnvl(o.cntry_cd = n.cntry_cd) or
                         lnnvl(o.st_fips_cd = n.st_fips_cd) or
                         lnnvl(o.cnty_fips_cd = n.cnty_fips_cd)
when not matched then insert (station_source, station_id, site_id, latitude, longitude, hrdat_uid, huc, cntry_cd, st_fips_cd, cnty_fips_cd, geom)
                      values (n.station_source, n.station_id, n.site_id, n.latitude, n.longitude, n.hrdat_uid, n.huc, n.cntry_cd, n.st_fips_cd,
                              n.cnty_fips_cd, n.geom);
commit;

prompt updating temporary table wqx_station_local from storetw
delete from wqx_station_local
 where station_source = 'STORETW' and
       not exists (select null
                     from station_no_source
                    where wqx_station_local.station_id = station_no_source.station_id);
commit;
merge into wqx_station_local o 
      using (select 'STORETW' station_source,
                    station_id,
                    site_id,
                    latitude,
                    longitude,
                    huc,
                    regexp_substr(governmental_unit_code, '[^:]+') cntry_cd,
                    regexp_substr(governmental_unit_code, '[^:]+', 1, 2) st_fips_cd,
                    regexp_substr(governmental_unit_code, '[^:]+', 1, 3) cnty_fips_cd,
                    geom
               from station_no_source
              where station_no_source.site_id not in (select site_id from wqx_station_local)
            ) n
  on (o.station_source = n.station_source and
      o.station_id = n.station_id)
when matched then update
                     set o.site_id = n.site_id,
                         o.latitude = n.latitude,
                         o.longitude = n.longitude,
                         o.huc = n.huc,
                         o.cntry_cd = n.cntry_cd,
                         o.st_fips_cd = n.st_fips_cd,
                         o.cnty_fips_cd = n.cnty_fips_cd,
                         o.calculated_huc_12 = null,
                         o.calculated_fips = null,
                         o.geom = n.geom
                   where lnnvl(o.latitude = n.latitude) or
                         lnnvl(o.longitude = n.longitude) or
                         lnnvl(o.huc = n.huc) or
                         lnnvl(o.cntry_cd = n.cntry_cd) or
                         lnnvl(o.st_fips_cd = n.st_fips_cd) or
                         lnnvl(o.cnty_fips_cd = n.cnty_fips_cd)
when not matched then insert (station_source, station_id, site_id, latitude, longitude, huc, cntry_cd, st_fips_cd, cnty_fips_cd, geom)
                      values (n.station_source, n.station_id, n.site_id, n.latitude, n.longitude, n.huc, n.cntry_cd, n.st_fips_cd,
                              n.cnty_fips_cd, n.geom);
commit;

--!!!!!!!!!!!!ONLY US/(US Teritory) stations are in the huc lookup table (mostly - some overlap on borders)!!!!!!!!!!!!!!!!!!!
prompt calculating huc
update wqx_station_local 
   set calculated_huc_12 = (select huc8
                              from huc8_geom_lookup
                             where sdo_contains(huc8_geom_lookup.geom,
	                                            wqx_station_local.geom) = 'TRUE')
 where calculated_huc_12 is null;
commit;

--!!!!!!!!!!!!ONLY US/(US Teritory) stations are in the county lookup tables (WQX does identify some US Teritories as a country)!!!!!!!!!!!!!!!!!!!
prompt calculating geopolitical data
update wqx_station_local 
   set calculated_fips = (select statefp || countyfp
                            from county_geom_lookup
                           where sdo_contains(county_geom_lookup.geom,
					                          wqx_station_local.geom) = 'TRUE')
 where calculated_fips is null and
       cntry_cd in ('AS','PR','UM','US', 'VI');
commit;

prompt dropping storet station indexes
exec etl_helper.drop_indexes('station_swap_storet');
        
prompt populating storet station
truncate table station_swap_storet;

insert /*+ append parallel(4) */
  into station_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                            geom, station_name, organization_name, description_text, station_type_name, latitude, longitude, map_scale,
                            geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                            geoposition_accy_value, geoposition_accy_unit
                           )
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select monitoring_location.mloc_uid station_id,
               org.org_id || '-' || monitoring_location.mloc_id site_id,
               org.org_id organization,
   	           wqx_site_type_conversion.station_group_type site_type,
               nvl(wqx_station_local.calculated_huc_12, nvl(mloc_huc_12, mloc_huc_8)) huc,
               case
                 when wqx_station_local.calculated_fips is null or
                      substr(wqx_station_local.calculated_fips, 3) = '000'
                   then wqx_station_local.cntry_cd || ':' || wqx_station_local.st_fips_cd || ':' || wqx_station_local.cnty_fips_cd
                 else 'US:' || substr(wqx_station_local.calculated_fips, 1, 2) || ':' || substr(wqx_station_local.calculated_fips, 3, 3)
               end governmental_unit_code, 
               wqx_station_local.geom,
               trim(monitoring_location.mloc_name) station_name,
               org.org_name organization_name,
               trim(monitoring_location.mloc_desc) description_text,
               monitoring_location_type.mltyp_name station_type_name,
               monitoring_location.mloc_latitude latitude,
               monitoring_location.mloc_longitude longitude,
               cast(monitoring_location.mloc_source_map_scale as varchar2(4000 char)) map_scale,
               nvl(horizontal_collection_method.hcmth_name, 'Unknown') geopositioning_method,
               nvl(horizontal_reference_datum.hrdat_name, 'Unknown') hdatum_id_code,
               monitoring_location.mloc_vertical_measure elevation_value,
               nvl2(monitoring_location.mloc_vertical_measure, nvl(measurement_unit.msunt_cd, 'ft'), null) elevation_unit,
               nvl2(monitoring_location.mloc_vertical_measure, vertical_collection_method.vcmth_name, null) elevation_method,
               nvl2(monitoring_location.mloc_vertical_measure, vertical_reference_datum.vrdat_name, null) vdatum_id_code,
               monitoring_location.mloc_horizontal_accuracy geoposition_accy_value,
               hmeasurement_unit.msunt_cd geoposition_accy_unit
          from wqx.monitoring_location
               left join wqx_station_local
                 on monitoring_location.mloc_uid = wqx_station_local.station_id and
                    'WQX' = wqx_station_local.station_source
               left join wqx.vertical_reference_datum
                 on monitoring_location.vrdat_uid = vertical_reference_datum.vrdat_uid
               left join wqx.vertical_collection_method
                 on monitoring_location.vcmth_uid = vertical_collection_method.vcmth_uid
               left join wqx.measurement_unit
                 on monitoring_location.msunt_uid_vertical_measure = measurement_unit.msunt_uid
               left join wqx.measurement_unit hmeasurement_unit
                 on monitoring_location.msunt_uid_horizontal_accuracy = hmeasurement_unit.msunt_uid
               left join wqx.horizontal_reference_datum
                 on monitoring_location.hrdat_uid = horizontal_reference_datum.hrdat_uid
               left join wqx.horizontal_collection_method
                 on monitoring_location.hcmth_uid = horizontal_collection_method.hcmth_uid
               left join wqx.organization org
                 on monitoring_location.org_uid = org.org_uid
               left join wqx.monitoring_location_type
                 on monitoring_location.mltyp_uid = monitoring_location_type.mltyp_uid
               left join wqx_site_type_conversion
                 on monitoring_location.mltyp_uid = wqx_site_type_conversion.mltyp_uid
         where org.org_id not like '%TEST%' and
               org.org_id not like '%TRAINING%'
        union all 
        select wqx_station_local.station_id + 10000000 station_id,
               station_no_source.site_id,
               station_no_source.organization,
           	   station_no_source.site_type,
               nvl(wqx_station_local.calculated_huc_12, wqx_station_local.huc) huc,
               case
                 when wqx_station_local.calculated_fips is null or
                      substr(wqx_station_local.calculated_fips, 3) = '000'
                   then wqx_station_local.cntry_cd || ':' || wqx_station_local.st_fips_cd || ':' || wqx_station_local.cnty_fips_cd
                 else 'US:' || substr(wqx_station_local.calculated_fips, 1, 2) || ':' || substr(wqx_station_local.calculated_fips, 3, 3)
               end governmental_unit_code, 
               wqx_station_local.geom,
               station_no_source.station_name,
               station_no_source.organization_name,
               station_no_source.description_text,
               station_no_source.station_type_name,
               wqx_station_local.latitude,
               wqx_station_local.longitude,
               station_no_source.map_scale,
               station_no_source.geopositioning_method,
               station_no_source.hdatum_id_code,
               station_no_source.elevation_value,
               station_no_source.elevation_unit,
               station_no_source.elevation_method,
               station_no_source.vdatum_id_code,
               null geoposition_accy_value,
               null geoposition_accy_unit
          from wqx_station_local
               join station_no_source
                 on wqx_station_local.site_id = station_no_source.site_id
         where wqx_station_local.station_source = 'STORETW'
        ) a
    order by organization;

commit;

select 'transform station end time: ' || systimestamp from dual;
