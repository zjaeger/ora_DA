-- da_set_IN.sql

prompt >>> alter table DA_SET add constraint DA_SET_PK

alter table DA_SET add
constraint DA_SET_PK primary key( SET_KEY )
using index
tablespace &TABSP_NAME
/

prompt >>> alter table DA_SET add constraint DA_SET_UK

alter table DA_SET add
constraint DA_SET_UK unique( SET_NAME )
using index
tablespace &TABSP_NAME
/
