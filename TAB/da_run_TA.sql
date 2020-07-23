-- da_run_TA.sql

prompt >>> create table DA_RUN

create table DA_RUN(
  RUN_KEY                         NUMBER               NOT NULL,
  RUN_NAME                        VARCHAR2(30)         NOT NULL,
  RUN_OWNER                       VARCHAR2(30)         NULL,
  RUN_MASK_LIKE                   VARCHAR2(100)        NULL,
  RUN_MASK_NOTLIKE                VARCHAR2(100)        NULL,
  RUN_MASK_REGEXP_LIKE            VARCHAR2(100)        NULL,
--RUN_ENUM_FLAG                   VARCHAR2(1)          DEFAULT 'N' NULL,
--RUN_DB_LINK                     VARCHAR2(30)         NULL,
  RUN_DESC                        VARCHAR2(2000)       NULL,
  INSERTED_DATE                   DATE                 NULL
)
tablespace &TABSP_NAME
/

COMMENT ON TABLE DA_RUN IS
'Domain analysis schema'
/
-- COMMENT ON COLUMN DA_RUN.RUN_DB_LINK IS
-- 'Schema default database link'
-- /
COMMENT ON COLUMN DA_RUN.RUN_DESC IS
'Schema description description'
/
-- COMMENT ON COLUMN DA_RUN.RUN_ENUM_FLAG IS
-- 'Schema based on enumeration of tables (not gathered automatically)'
-- /
COMMENT ON COLUMN DA_RUN.RUN_KEY IS
'Surrogate primary key of the DA_RUN table'
/
COMMENT ON COLUMN DA_RUN.INSERTED_DATE IS
'Schema last analysis date'
/
COMMENT ON COLUMN DA_RUN.RUN_MASK_LIKE IS
'Schema dynamically gathered tables like condition'
/
COMMENT ON COLUMN DA_RUN.RUN_NAME IS
'Schema name'
/
COMMENT ON COLUMN DA_RUN.RUN_MASK_NOTLIKE IS
'Schema dynamically gathered tables not like condition'
/
COMMENT ON COLUMN DA_RUN.RUN_OWNER IS
'Schema default owner'
/
