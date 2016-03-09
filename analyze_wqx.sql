show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'analyze wqx tables start time: ' || systimestamp from dual;

begin
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'MONITORING_LOCATION', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ORGANIZATION', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'COUNTRY', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'STATE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'COUNTY', method_opt => 'FOR ALL INDEXED COLUMNS');
	
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'VERTICAL_REFERENCE_DATUM', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'VERTICAL_COLLECTION_METHOD', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'MEASUREMENT_UNIT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'HORIZONTAL_REFERENCE_DATUM', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'HORIZONTAL_COLLECTION_METHOD', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'MONITORING_LOCATION_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY_PROJECT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'PROJECT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ANALYTICAL_METHOD', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ANALYTICAL_METHOD_CONTEXT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_DETECT_QUANT_LIMIT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'DETECTION_QUANT_LIMIT_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'SAMPLE_COLLECTION_EQUIP', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY_CONDUCTING_ORG', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'TIME_ZONE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY_MEDIA_SUBDIVISION', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY_MEDIA', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ASSEMBLAGE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'HORIZONTAL_COLLECTION_METHOD', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'HORIZONTAL_REFERENCE_DATUM', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'NET_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'TOXICITY_TEST_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'CONTAINER_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'CONTAINER_COLOR', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'THERMAL_PRESERVATIVE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RELATIVE_DEPTH', method_opt => 'FOR ALL INDEXED COLUMNS');
	
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'METHOD_SPECIATION', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'BIOLOGICAL_INTENT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'CHARACTERISTIC', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_DETECTION_CONDITION', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'SAMPLE_FRACTION', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_MEASURE_QUALIFIER', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_STATUS', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_STATISTICAL_BASE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_VALUE_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_WEIGHT_BASIS', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_TIME_BASIS', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_TEMPERATURE_BASIS', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'TAXON', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'SAMPLE_TISSUE_ANATOMY', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_LAB_COMMENT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_TAXON_HABIT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'HABIT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_TAXON_DETAIL', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'VOLTINISM', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_TAXON_FEEDING_GROUP', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'CITATION', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'CELL_FORM', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'CELL_SHAPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY_METRIC', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'ACTIVITY_METRIC_INDEX', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'BIOLOGICAL_HABITAT_INDEX', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'FREQUENCY_CLASS_DESCRIPTOR', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'METRIC_TYPE', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'METRIC_TYPE_CONTEXT', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_FREQUENCY_CLASS', method_opt => 'FOR ALL INDEXED COLUMNS');
	dbms_stats.gather_table_stats(ownname => 'WQX', tabname => 'RESULT_LAB_SAMPLE_PREP', method_opt => 'FOR ALL INDEXED COLUMNS');
end;
/

select 'analyze wqx tables end time: ' || systimestamp from dual;
