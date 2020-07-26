-- DA_create_user.sql
--
-- create user DTEST (run as DBA user)

define UNAME = DTEST

set echo on

create user &UNAME identified by dtest default tablespace USERS temporary tablespace TEMP
/

set feedback off

grant create session   to &UNAME ;
grant alter  session   to &UNAME ;
grant create table     to &UNAME ;
grant create view      to &UNAME ;
grant create procedure to &UNAME ;
grant create sequence  to &UNAME ;
grant create job       to &UNAME ;

set feedback on

alter user &UNAME quota unlimited on USERS
/

set echo off
