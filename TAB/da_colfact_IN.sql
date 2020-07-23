-- da_colfact_IN.sql

prompt >>> alter table DA_COLFACT add constraint DA_COLFACK_PK

alter table DA_COLFACT add
constraint DA_COLFACT_PK primary key( COLFACT_KEY )
using index
tablespace &TABSP_NAME
/

prompt >>> alter table DA_COLFACT add constraint DA_COLFACK_UK

alter table DA_COLFACT add
-- constraint DA_COLFACT_UK unique( COL_KEY, COLFACT_METRIC, COLFACT_DIRECTION, COLFACT_BY, COLFACT_ORDER )
constraint DA_COLFACT_UK unique( COL_KEY, COLFACT_METRIC, COLFACT_ORDER )
using index
tablespace &TABSP_NAME
/
