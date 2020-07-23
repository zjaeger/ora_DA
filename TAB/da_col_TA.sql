-- da_col_TA.sql

prompt >>> create table DA_COL

create table DA_COL(
  col_key                         NUMBER(12)           NOT NULL,
  tab_key                         NUMBER(12)           NOT NULL,
  col_name                        VARCHAR2(30)         NOT NULL,
  col_mandatory                   VARCHAR2(1)          NULL,
  col_pk                          NUMBER(2)            NULL,
  col_seqno                       NUMBER(6)            NOT NULL,
  col_data_type                   VARCHAR2(30)         NOT NULL,
  col_data_type2                  CHAR(1)              NOT NULL,
  col_data_length                 NUMBER(6)            NULL,
  col_data_precision              NUMBER(6)            NULL,
  col_data_scale                  NUMBER(6)            NULL,
  col_desc                        VARCHAR2(2000)       NULL,
  col_default                     VARCHAR2(2000)       NULL,
  col_cnt_all                     NUMBER(12)           NULL,
  col_cnt_distinct                NUMBER(12)           NULL,
  col_cnt_no_default              NUMBER(12)           NULL,
  col_len_min                     NUMBER(6)            NULL,
  col_len_max                     NUMBER(6)            NULL,
  inserted_date                   DATE                 NULL,
  updated_date                    DATE                 NULL
)
tablespace &TABSP_NAME
/

comment on table DA_COL is
'Anayzed column'
/
comment on column DA_COL.COL_CNT_DISTINCT is
'Number distinct values'
/
comment on column DA_COL.COL_DATA_LENGTH is
'Data type - length'
/
comment on column DA_COL.COL_DATA_PRECISION is
'Data type - precision'
/
comment on column DA_COL.COL_DATA_SCALE is
'Data type - scale'
/
comment on column DA_COL.COL_DATA_TYPE is
'Data type'
/
comment on column DA_COL.COL_DEFAULT is
'Column default'
/
comment on column DA_COL.COL_DESC is
'Column description'
/
comment on column DA_COL.COL_KEY is
'Surrogate primary key of the DA_COL table'
/
comment on column DA_COL.updated_date is
'Column last update date'
/
comment on column DA_COL.COL_MANDATORY is
'Mandatory flag'
/
comment on column DA_COL.COL_NAME is
'Column name'
/
comment on column DA_COL.COL_PK is
'Primary key order'
/
comment on column DA_COL.COL_CNT_ALL is
'Number of filled values'
/
comment on column DA_COL.TAB_KEY is
'Table foreign key'
/
