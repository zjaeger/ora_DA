-- da_log_TA.sql

prompt >>> create table DA_LOG

create table DA_LOG(
  log_time                        DATE                 NOT NULL,
  log_type                        CHAR(1)              NOT NULL,  -- E: error, W: warning, I: info
  log_msg                         VARCHAR2(200)        NOT NULL,
  log_stack                       CLOB                 NULL,
  set_key                         NUMBER(6)            NULL,
  tab_key                         NUMBER(12)           NULL,
  col_key                         NUMBER(12)           NULL
)
tablespace &TABSP_NAME
/
