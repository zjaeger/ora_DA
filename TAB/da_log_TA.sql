-- da_log_TA.sql

prompt >>> create table DA_LOG

create table DA_LOG(
  log_time                        DATE                 NOT NULL,
  log_type                        CHAR(1)              NOT NULL,
  log_msg                         VARCHAR2(200)        NOT NULL,
  log_stack                       CLOB                 NULL,
  RUN_KEY                         NUMBER(6)            NULL,
  TAB_KEY                         NUMBER(12)           NULL,
  COL_KEY                         NUMBER(12)           NULL
)
tablespace &TABSP_NAME
/
