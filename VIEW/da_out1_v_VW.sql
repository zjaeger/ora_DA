-- da_out1_v_VW.sql

prompt >>> create view DA_OUT1_V

create or replace view DA_OUT1_V
as
with
X_VALUES
as
  ( select
      j.COL_KEY,
      j.COLFACT_METRIC,
      j.COLFACT_ORDER,
      j.COLFACT_ORDER - 1 - j.COLFACT_ORDER_MAX as COLFACT_ORDER_DESC,
      j.COLFACT_COUNT,
      j.COLFACT_VALUE
    from
      ( select
          i.COL_KEY,
          i.COLFACT_METRIC,
          i.COLFACT_ORDER,
          i.COLFACT_COUNT,
          i.COLFACT_VALUE,
          max( i.COLFACT_ORDER ) over (partition by i.COL_KEY, i.COLFACT_METRIC) as COLFACT_ORDER_MAX
        from
          DA_COLFACT i
      ) j
    where
         j.COLFACT_ORDER = 1                   -- the first (minimum)
      or j.COLFACT_ORDER = j.COLFACT_ORDER_MAX -- the last  (maximum)
  )
select
  a.RUN_KEY,
  a.RUN_OWNER as OWNER,
  b.TAB_NAME,
  c.COL_KEY,
  c.COL_SEQNO,
  c.COL_NAME,
  b.TAB_NUM_ROWS,           -- table rows count
  c.COL_CNT_ALL as COL_CNT, -- not null values count
  c.COL_CNT_DISTINCT,       -- distinct values count
  case
    when c.COL_CNT_ALL > 0
    then to_char( round( c.COL_CNT_DISTINCT / b.TAB_NUM_ROWS * 100, 2 ),'FM990D00')||'%'
  end as CARDINALITY,
  c.COL_CNT_NO_DEFAULT,
  -- value
  d.COLFACT_VALUE as VAL_MIN,     -- minimum
  d.COLFACT_COUNT as VAL_MIN_CNT,
  e.COLFACT_VALUE as VAL_MAX,     -- maximum
  e.COLFACT_COUNT as VAL_MAX_CNT,
  -- frequency
  f.COLFACT_VALUE as FREQ_MIN_VAL, -- least frequent
  f.COLFACT_COUNT as FREQ_MIN_CNT,
  g.COLFACT_VALUE as FREQ_MAX_VAL, -- most frequent
  g.COLFACT_COUNT as FREQ_MAX_CNT
from
  DA_RUN a
  inner join DA_TAB b        on ( a.RUN_KEY  = b.RUN_KEY )
  inner join DA_COL c        on ( b.TAB_KEY  = c.TAB_KEY
                                )
  left outer join X_VALUES d on ( c.COL_KEY  = d.COL_KEY
                                  and  'V'   = d.COLFACT_METRIC
                                  and   1    = d.COLFACT_ORDER
                                )
  left outer join X_VALUES e on ( c.COL_KEY  = e.COL_KEY
                                  and  'V'   = e.COLFACT_METRIC
                                  and  -1    = e.COLFACT_ORDER_DESC
                                )
  left outer join X_VALUES f on ( c.COL_KEY  = f.COL_KEY
                                  and  'F'   = f.COLFACT_METRIC
                                  and   1    = f.COLFACT_ORDER
                                )
  left outer join X_VALUES g on ( c.COL_KEY  = g.COL_KEY
                                  and  'F'   = g.COLFACT_METRIC
                                  and  -1    = g.COLFACT_ORDER_DESC
                                )
order by
  a.RUN_KEY,
  b.TAB_NAME,
  c.COL_SEQNO
/
