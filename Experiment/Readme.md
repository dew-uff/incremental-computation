For organization, project is divided into 5 structures: 
ETL (SQL code), Postgres (schema, metadata, etc), Results (experiments results in thesis), Scripts (python scripts), Tools (for clean tables, add or drop index, etc) python scripts and sql scripts.


The Python scripts execute the procedures contained in the SQL scripts in an orderly manner until the end of the processing of 21 update batches.
All codes (python scripts, tools, sql codes) running in the same dir.
SQL scripts contain ETL processes to extract, transform, and load data in different methods.
Please configure DBMS into python script.


## POSTGRES

Contains schema, metadata and .csv data for load tables. Input table requires running 'create startbase' and 'create updates'.


## SCRIPTS

### execute_standard_experiment 

- Python Script for running ETL INTEGRAL in NORMAL MODE, executing full queries - therefore, it is not /
possible to observe select and load times separately.
Requires SCRIPTA1.


### execute_first_incremental
- Python Script for running ETL INTEGRAL in INCREMENTAL START BASE (1 time) /
(20 times) executing full queries - therefore, it is not possible to observe extract/transform/load times separately.
Requires SCRIPTB1.
- 

### execute_inc1_experiment

- Python Script for running ETL in PURE INCREMENTAL MODE, /
(20 times) executing extract/transform/load separately, step by step (1%).
Requires SCRIPTB2.

### execute_inc2_experiment
- Python Script for running ETL in PURE INCREMENTAL MODE, /
(20 times) executing extract/transform/load separately, /
in variable batch size based on date.
Requires SCRIPTB2.



## ETL


### STANDARD APPROACH

- SCRIPTA1 - ETL standard, normal mode, code embedded in script1 - script3.




### INCREMENTAL APPROACH

- SCRIPTB1 - ETL standard adapted for subsequente reexecutions /
start base, code embedded in script1 - script3.

- SCRIPTB2 - ETL Incremental for reexecutions, code embedded in script1 - script3.



## TOOLS

### CLEAN TABLES

- Script for clean up execution control table and aggregation table.

### STATISTICS

- Executes statistics about entire base.


## RESULTS

Results presented in thesis, you can validate it executing yourself!  