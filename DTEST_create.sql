-- DTEST_create.sql
--
-- create user DTEST

set echo on

create user DTEST identified by dtest default tablespace USERS temporary tablespace TEMP
/

set feedback off

grant create session   to DTEST ;
grant alter  session   to DTEST ;
grant create table     to DTEST ;
grant create view      to DTEST ;
grant create procedure to DTEST ;
grant create sequence  to DTEST ;
grant create job       to DTEST ;

set feedback on

alter user DTEST quota unlimited on USERS
/

set echo off
