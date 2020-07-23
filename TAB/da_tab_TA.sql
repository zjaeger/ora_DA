-- da_tab_TA.sql

prompt >>> create table DA_TAB(

create table DA_TAB(
  tab_key                         NUMBER(12)           NOT NULL,
  run_key                         NUMBER(12)           NOT NULL,
  tab_par_key                     NUMBER(12)           NULL,
--tab_owner                       VARCHAR2(30)         NOT NULL,
  tab_name                        VARCHAR2(30)         NOT NULL,
--tab_partition                   VARCHAR2(30)         NULL,
--tab_partitioning_condition      VARCHAR2(2000)       NULL,
--tab_db_link                     VARCHAR2(30)         NULL,
  tab_num_rows                    NUMBER(12)           NULL,
  tab_desc                        VARCHAR2(2000)       NULL,
  tab_child_count                 NUMBER(12)           NULL,
  inserted_date                   DATE                 NULL,
  updated_date                    DATE                 NULL,
  tab_calc_time_start             DATE                 NULL,
  tab_calc_time_end               DATE                 NULL
)
tablespace &TABSP_NAME
/

comment on table DA_TAB is
'Anayzed table'
/
comment on column DA_TAB.RUN_KEY is
'RUN foreign key'
/
comment on column DA_TAB.TAB_CHILD_COUNT is
'Number of child tables'
/
-- comment on column DA_TAB.TAB_DB_LINK is
-- 'Table db-link if remote'
-- /
comment on column DA_TAB.TAB_DESC is
'Table description'
/
comment on column DA_TAB.TAB_KEY is
'Surrogate primary key of the DA_TAB table'
/
comment on column DA_TAB.INSERTED_DATE is
'Table last analysis date'
/
comment on column DA_TAB.TAB_NAME is
'Table name'
/
-- comment on column DA_TAB.TAB_OWNER is
-- 'Table owner'
-- /
-- comment on column DA_TAB.TAB_PARTITION is
-- 'Table partition or partition mask'
-- /
-- comment on column DA_TAB.TAB_PARTITIONING_CONDITION is
-- 'Condition overriding partition predicate (for linked tables)'
-- /
comment on column DA_TAB.TAB_PAR_KEY is
'Parent table foreign key'
/
comment on column DA_TAB.TAB_NUM_ROWS is
'Number of records'
/
