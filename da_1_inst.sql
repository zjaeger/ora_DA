-- da_1_inst.sql
--
-- recreate script for DA_% database objects

spool da_1_inst.lst

col DATABASE for a30
col USERNAME for a30
col CURR_DATE for a20
select
  upper( sys_context('USERENV','DB_NAME'))||'.'||
  sys_context('USERENV','DB_DOMAIN') as DATABASE,
  a.USER_ID                          as USR_ID,
  a.USERNAME                         as USERNAME,
  to_char( sysdate,'YYYY-MM-DD HH24:MI:SS') as CURR_DATE
from
  USER_USERS a
/

-- drop tables/sequences if exists
set serveroutput on
declare
  cursor c_seq is
    select 'drop sequence '||a.SEQUENCE_NAME as CMD
    from   USER_SEQUENCES a
    where  a.SEQUENCE_NAME like 'DA#_%#_SQ' escape '#' ;

  cursor c_tab is
    select
      'drop table '||b.TABLE_NAME as CMD
    from
      ( select
          a.TABLE_NAME,
          case a.TABLE_NAME
            when 'DA_COLFACT' then 1
            when 'DA_COL'     then 2
            when 'DA_TAB'     then 3
            when 'DA_RUN'     then 4
            when 'DA_LOG'     then 5
          end as TAB_PRIO
        from
          USER_TABLES a
        where
          a.TABLE_NAME like 'DA#_%' escape '#'
      ) b
    where
      b.TAB_PRIO is not null
    order by
      b.TAB_PRIO ;
begin
  for r in c_seq
  loop
    dbms_output.PUT_LINE('>> '||r.CMD ) ;
    execute immediate r.CMD ;
  end loop ;

  for r in c_tab
  loop
    dbms_output.PUT_LINE('>> '||r.CMD ) ;
    execute immediate r.CMD ;
  end loop ;
end ;
/

set feedback off

-- create tables

define TABSP_NAME = USERS

@tab/da_log_TA.sql
@tab/da_run_TA.sql
@tab/da_tab_TA.sql
@tab/da_col_TA.sql
@tab/da_colfact_TA.sql

-- create indexes

define TABSP_NAME = USERS

-- @da_log_IN.sql
@tab/da_run_IN.sql
@tab/da_tab_IN.sql
@tab/da_col_IN.sql
@tab/da_colfact_IN.sql

-- create FK constraints
@da_2_FK.sql

-- create sequences
@da_3_SQ.sql

-- create views
@view/da_out1_v_VW.sql

-- PL/SQL package
@pls/da_proc_p.pls

set feedback on

spool off
