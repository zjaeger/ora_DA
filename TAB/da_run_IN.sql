-- da_run_IN.sql

prompt >>> alter table DA_TAB add constraint DA_TAB_PK

alter table DA_RUN add
constraint DA_RUN_PK primary key( RUN_KEY )
using index
tablespace &TABSP_NAME
/

prompt >>> alter table DA_RUN add constraint DA_RUN_UK

alter table DA_RUN add
constraint DA_RUN_UK unique( RUN_NAME )
using index
tablespace &TABSP_NAME
/
