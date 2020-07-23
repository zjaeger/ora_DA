-- da_col_IN.sql

prompt >>> alter table DA_COL add constraint DA_COL_PK

alter table DA_COL add
constraint DA_COL_PK primary key( COL_KEY )
using index
tablespace &TABSP_NAME
/

prompt >>> alter table DA_COL add constraint DA_COL_UK

alter table DA_COL add
constraint DA_COL_UK unique( TAB_KEY, COL_NAME )
using index
tablespace &TABSP_NAME
/
