# ora_DA
Domain analysis for Oracle database

	Domain analysis is a task of data profiling allowing gather automatically some kind of statistical metadata of either data in their original database schema or data samples loaded into an Oracle database.

	Simple module gathers data into own metadata tables.

## Setup

Tested on local database [Oracle 18 XE](https://www.oracle.com/database/technologies/appdev/xe.html)

### Create schema DTEST (optional)

		CMD> cd ora_DA
		CMD> sqlplus /nolog
		sql> conn system@localhost/XEPDB1
		sql> @DTEST_create.sql

### Create DA metadata tables, views, sequences, packages

		sql> conn dtest/dtest@localhost/XEPDB1
		sql> @da_1_inst.sql

Check da_1_inst.lst file for errors.

### Example: run Domain Analysis module for schema HR (Oracle sample schema)

#### Set privileges for read data from schema HR for user DTEST

		sql> conn HR@localhost/XEPDB1
		sql> select 'grant select on '||TABLE_NAME||' to DTEST ;' as X from USER_TABLES order by TABLE_NAME ;

Run generated grant commands:

		sql> grant select on COUNTRIES to DTEST ;
		sql> grant select on DEPARTMENTS to DTEST ;
		sql> grant select on EMPLOYEES to DTEST ;
		sql> grant select on JOB_HISTORY to DTEST ;
		sql> grant select on JOBS to DTEST ;
		sql> grant select on LOCATIONS to DTEST ;
		sql> grant select on REGIONS to DTEST ;

#### Prepare for test of schema HR

		sql> begin
		  2    DA_PROC_P.init(
		  3      p_name        => 'HR_all',
		  4      p_owner       => 'HR',
		  5      p_mask_like   => '%',
		  6      p_description => 'HR, all tables') ;
		  7  end ;
		  8  /
		>> da_run.RUN_KEY = 1
		7 - rows merged into DA_TAB.
		35 - rows merged into DA_COL.
		0 - rows updated for DA_COL.col_default.

#### Run Domain Analysis test for prepared dataset

		sql> exec DA_PROC_P.go( p_run_key => 1 ) ;
		>> COUNTRIES
		>> DEPARTMENTS
		>> EMPLOYEES
		>> JOBS
		>> JOB_HISTORY
		>> LOCATIONS
		>> REGIONS

#### Output Report

		sql> select * from DA_OUT1_V ;
