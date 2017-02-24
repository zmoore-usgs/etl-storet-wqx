# etl\-storet\-wqx

ETL Water Quality Data from the STORET WQX System

These scripts are run by the OWI Jenkins Job Runners. The job name is WQP\_STORET\_WQX\_ETL. They follow the general OWI ETL pattern using ant to control the execution of PL/SQL scripts.

The basic flow is:

* Copy the data download and cleanup scripts to the database (nolog) server.

* Make sure they have the correct end-of-line characters.

* Download data files as needed. (wqx_dump.sh)

* Import the data into the wqx schema of the nolog database using impdp.

* Grant select on the storetw tables to wqp\_core and analyze them. (wqx\_grants\_and\_analyze.sql)

* Drop the referential integrity constraints on the biodata swap tables of the wqp_core schema. (dropRI.sql)

* Drop the indexes on the storet station swap table, populate with transformed data, and rebuild the indexes. (transformStation.sql)

* Drop the indexes on the storet activity swap table, populate with transformed data, and rebuild the indexes. (transformActivity.sql)

* Drop the indexes on the storet result swap table, populate with transformed data, and rebuild the indexes. (transformResult.sql)

* Drop the indexes on the storet summary swap tables, populate with transformed data, and rebuild the indexes. (createSummaries.sql)

* Drop the indexes on the storet code lookup swap tables, populate with transformed data, and rebuild the indexes. (createCodes.sql)

**Note:** Several code lookup values are dependent on data from the WQP\_NWIS\_ETL correctly collecting data from natprod.


* Add back the referential integrity constraints on the storet swap tables of the wqp_core schema. (addRI.sql)

* Analyze the storet swap tables of the wqp_core schema. (analyze.sql)

* Validate that rows counts and change in row counts are withing the tolerated values. (validate.sql)

* Install the new data using partition exchanges. (install.sql)

* Mark the current version of data as processed. (wqx\_finish.sh)

The translation of data is specific to this repository. The heavy lifting (indexing, RI, partition exchanges, etc.) is done using common packages in the wqp_core schema. These are defined in the schema-wqp-core repository.
