-- da_SQ.sql
--
-- create sequences

prompt >>> create sequence DA_SET_KEY_SQ

create sequence DA_SET_KEY_SQ
increment by 1
start with 1
nocycle
nocache
/

prompt >>> create sequence DA_TAB_KEY_SQ

create sequence DA_TAB_KEY_SQ
increment by 1
start with 1
nocycle
cache 16
/

prompt >>> create sequence DA_COL_KEY_SQ

create sequence DA_COL_KEY_SQ
increment by 1
start with 1
nocycle
cache 64
/

prompt >>> create sequence DA_COLFACT_KEY_SQ

create sequence DA_COLFACT_KEY_SQ
increment by 1
start with 1
nocycle
cache 128
/
