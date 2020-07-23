-- da_2_FK.sql
--
-- Foreign key constraints

prompt >>> alter table DA_TAB add constraint DA_TAB_RUN_FK

alter table DA_TAB add
constraint DA_TAB_RUN_FK foreign key (RUN_KEY)
references DA_RUN (RUN_KEY) on delete cascade
/

prompt >>> alter table DA_COL add constraint DA_COL_TAB_FK

alter table DA_COL add
constraint DA_COL_TAB_FK foreign key (TAB_KEY)
references DA_TAB (TAB_KEY) on delete cascade
/

prompt >>> alter table DA_COLFACT add constraint DA_COLFACT_COL_FK

alter table DA_COLFACT add
constraint DA_COLFACT_COL_FK foreign key (COL_KEY)
references DA_COL (COL_KEY) on delete cascade
/
