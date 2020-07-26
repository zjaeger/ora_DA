-- da_set_TA.sql

prompt >>> create table DA_SET

create table DA_SET(
  SET_KEY                         NUMBER               NOT NULL,
  SET_NAME                        VARCHAR2(30)         NOT NULL,
  SET_OWNER                       VARCHAR2(30)         NULL,
  SET_MASK_TP                     CHAR(1)              NOT NULL, -- L: like, R: regexp_like
  SET_MASK_LIKE                   VARCHAR2(100)        NULL,
  SET_MASK_NOTLIKE                VARCHAR2(100)        NULL,
  SET_MASK_REGEXP_LIKE            VARCHAR2(100)        NULL,
--SET_ENUM_FLAG                   VARCHAR2(1)          DEFAULT 'N' NULL,
--SET_DB_LINK                     VARCHAR2(30)         NULL,
  SET_DBUSER                      VARCHAR2(30)         NULL,
  SET_OSUSER                      VARCHAR2(30)         NULL,
  SET_DESC                        VARCHAR2(2000)       NULL,
  INSERTED_DATE                   DATE                 NULL
)
tablespace &TABSP_NAME
/

COMMENT ON TABLE DA_SET IS
'Domain analysis schema'
/
-- COMMENT ON COLUMN DA_SET.SET_DB_LINK IS
-- 'Schema default database link'
-- /
COMMENT ON COLUMN DA_SET.SET_DESC IS
'Schema description description'
/
-- COMMENT ON COLUMN DA_SET.SET_ENUM_FLAG IS
-- 'Schema based on enumeration of tables (not gathered automatically)'
-- /
COMMENT ON COLUMN DA_SET.SET_KEY IS
'Surrogate primary key of the DA_SET table'
/
COMMENT ON COLUMN DA_SET.INSERTED_DATE IS
'Schema last analysis date'
/
COMMENT ON COLUMN DA_SET.SET_MASK_LIKE IS
'Schema dynamically gathered tables like condition'
/
COMMENT ON COLUMN DA_SET.SET_NAME IS
'Schema name'
/
COMMENT ON COLUMN DA_SET.SET_MASK_NOTLIKE IS
'Schema dynamically gathered tables not like condition'
/
COMMENT ON COLUMN DA_SET.SET_OWNER IS
'Schema default owner'
/
