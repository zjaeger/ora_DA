-- DA_install.sql
--
-- recreate script for DA_% database objects

spool DA_install.lst

col DATABASE for a30
col USERNAME for a30
col CURR_DATE for a20
-- print current schema
select
  upper( sys_context('USERENV','DB_NAME'))||'.'||
  sys_context('USERENV','DB_DOMAIN') as DATABASE,
  a.USER_ID                          as USR_ID,
  a.USERNAME                         as USERNAME,
  to_char( sysdate,'YYYY-MM-DD HH24:MI:SS') as CURR_DATE
from
  USER_USERS a
/

-- drop DA_% tables/sequences if exists
set serveroutput on
declare
  cursor c_dbob is
    select 1 as S1, 0 as S2, 'drop sequence '||a.SEQUENCE_NAME as CMD
    from   USER_SEQUENCES a
    where  a.SEQUENCE_NAME like 'DA#_%#_SQ' escape '#'
    union all
    select 2 as S1, 0 as S2, 'drop view '||a.VIEW_NAME as CMD
    from   USER_VIEWS a
    where  a.VIEW_NAME like 'DA#_%#_V' escape '#'
    union all
    select
      3          as S1,
      b.TAB_PRIO as S2,
      'drop table '||b.TABLE_NAME as CMD
    from
      ( select
          a.TABLE_NAME,
          case a.TABLE_NAME
            when 'DA_COLFACT' then 1
            when 'DA_COL'     then 2
            when 'DA_TAB'     then 3
            when 'DA_SET'     then 4
            when 'DA_LOG'     then 5
                              else 6
          end as TAB_PRIO
        from
          USER_TABLES a
        where
          a.TABLE_NAME like 'DA#_%' escape '#'
      ) b
    order by 1, 2 ;
begin
  for r in c_dbob
  loop
    dbms_output.PUT_LINE('>> '||r.CMD ) ;
    begin
      execute immediate r.CMD ;
    exception
      when OTHERS then
        dbms_output.PUT_LINE( SQLERRM ) ;
    end ;
  end loop ;
end ;
/

set feedback off

-- create tables

-- define TABSP_NAME = USERS
define TABSP_NAME = SYM_DATA

@TAB/da_log_TA.sql
@TAB/da_set_TA.sql
@TAB/da_tab_TA.sql
@TAB/da_col_TA.sql
@TAB/da_colfact_TA.sql

-- create indexes

-- define TABSP_NAME = USERS
define TABSP_NAME = SYM_IND

-- @da_log_IN.sql
@TAB/da_set_IN.sql
@TAB/da_tab_IN.sql
@TAB/da_col_IN.sql
@TAB/da_colfact_IN.sql

-- create FK constraints
@TAB/da_01_FK.sql

-- create sequences
@OTH/da_02_SQ.sql

-- create views
@VIEW/da_out1_v_VW.sql
@VIEW/da_tab_run_v_VW.sql

-- PL/SQL package
@PLS/da_proc_p.pls

set feedback on

spool off
