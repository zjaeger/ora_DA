-- da_tab_run_v_VW.sql
--
-- 2020-05-28 - where condition removed

prompt >>> create view DA_TAB_RUN_V

create or replace view DA_TAB_RUN_V
as
select
  a.SET_KEY,
  a.TAB_KEY,
  a.TAB_NAME,
  a.TAB_DESC,
  a.TAB_NUM_ROWS,
  a.TAB_CALC_TIME_START,
  a.TAB_CALC_TIME_END,
  round( (a.TAB_CALC_TIME_END - a.TAB_CALC_TIME_START)*24*60, 2 ) as DUR_MIN
from
  DA_TAB a
order by
  a.SET_KEY,
  a.TAB_CALC_TIME_START nulls last,
  a.TAB_NAME
/
