-- da_colfact_TA.sql

prompt >>> create table DA_COLFACT

create table DA_COLFACT(
  COLFACT_KEY                     number(12)           not null,
  COL_KEY                         number(12)           not null,
  COLFACT_METRIC                  char(1)              not null, -- L (length), V (value)
--COLFACT_DIRECTION               char(1)              not null, -- T (top), B (bottom)
--COLFACT_BY                      char(1)              not null, -- V (value), F (frequency)
  COLFACT_ORDER                   number(12)           not null,
  COLFACT_COUNT                   number(12)           null,
  COLFACT_VALUE                   varchar2(512)        null,
  INSERTED_DATE                   date                 null
)
tablespace &TABSP_NAME
/

comment on table DA_COLFACT is
'Anayzed column facts'
/
-- comment on column DA_COLFACT.COLFACT_BY is
-- 'value/occurance (V - value, F - frequency, # - N/A)'
-- /
comment on column DA_COLFACT.COLFACT_COUNT is
'Number of the values'
/
-- comment on column DA_COLFACT.COLFACT_DIRECTION is
-- 'Top (T), Bottom (B), Max (M), N/A (#)'
-- /
comment on column DA_COLFACT.COLFACT_KEY is
'Surrogate primary key of the DA_COLFACT table'
/
comment on column DA_COLFACT.COLFACT_METRIC is
'Measured value type (L - length, V - value, # - N/A)'
/
comment on column DA_COLFACT.COLFACT_ORDER is
'Order of the value'
/
comment on column DA_COLFACT.COLFACT_VALUE is
'Value'
/
comment on column DA_COLFACT.COL_KEY is
'Column foreign key'
/
