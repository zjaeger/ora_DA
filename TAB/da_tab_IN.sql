-- da_tab_IN.sql

prompt >>> alter table DA_TAB add constraint DA_TAB_PK

alter table DA_TAB add
constraint DA_TAB_PK primary key( TAB_KEY )
using index
tablespace &TABSP_NAME
/

prompt >>> alter table DA_TAB add constraint DA_TAB_UK

alter table DA_TAB add
constraint DA_TAB_UK unique ( RUN_KEY, TAB_NAME )
-- constraint DA_TAB_UK unique ( RUN_KEY, TAB_NAME, TAB_OWNER )
using index
tablespace &TABSP_NAME
/

prompt >>> create index DA_TAB_PAR_KEY

create index DA_TAB_PAR_KEY on DA_TAB (TAB_PAR_KEY)
tablespace &TABSP_NAME
/
