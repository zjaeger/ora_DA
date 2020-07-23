-- da_proc_p.pls
--
-- alter session set NLS_SORT='CZECH' ;
-- alter session set NLS_COMP=LINGUISTIC ;

prompt >>> create package DA_PROC_P

create or replace package DA_PROC_P authid current_user
as
  ------------------------------------------------------------------------------
  -- Domain analysis module 
  ------------------------------------------------------------------------------
  -- CopyLeft, 2007, 2008, bjankovsky, zjaeger
  --
  -- 2007-08-14  bjankovsky  New module
  -- 2008-10-14  bjankovsky  Failover enhanced, remote analysis accelerated
  --                         (source code: http://www.bobjankovsky.org/show.php?seq=4)
  -- 2020-04-08  zjaeger     Redesign (move dynamic code to static code ...)
  ------------------------------------------------------------------------------

  procedure init( p_name             in varchar2,         -- any analysis identifier
                  p_owner            in varchar2 := null, -- analysed tables schema owner
                  p_mask_like        in varchar2 := null, -- analysed table names LIKE mask
                  p_mask_notlike     in varchar2 := null, -- analysed table names NOT LIKE mask
                  p_mask_regexp_like in varchar2 := null, -- analysed table names REGEXP_LIKE expression
               -- p_enum_flag        in varchar2 := 'N',
               -- p_db_link          in varchar2 := null,
                  p_description      in varchar2 := null ) ;

  procedure del( p_run_key in integer ) ;

  procedure go( p_run_key         in integer,
                p_tab_regexp_like in varchar2 := null,
                p_col_regexp_like in varchar2 := null
              ) ;

  procedure go_job_start( p_run_key in integer ) ;
  procedure go_job_stop(  p_run_key in integer ) ;

end DA_PROC_P ;
/

prompt >>> create package body DA_PROC_P

create or replace package body DA_PROC_P
as
  subtype t_varchar2_max is varchar2(32767) ;

  type t_TA_tab_key      is table of DA_TAB.tab_key%TYPE ;
  type t_TA_col_key      is table of DA_COL.col_key%TYPE ;
  type t_TA_col_name     is table of DA_COL.col_name%TYPE ;        -- Oracle column name
  type t_TA_col_type2    is table of DA_COL.col_data_type2%TYPE ;  -- derived one char data type
  type t_TA_col_len_db   is table of DA_COL.col_data_length%TYPE ; -- Oracle column data lenght max
  type t_TA_col_default  is table of DA_COL.col_default%TYPE ;     -- Oracle column default value
  type t_TA_col_cnt      is table of DA_COL.col_cnt_all%TYPE ;
  type t_TA_col_len      is table of DA_COL.col_len_max%TYPE ;

  C_COLFACT_VALUE_MAX constant number(4)   :=  512 ;    -- max length for DA_COLFACT.colfact_value
  C_COLFACT_LIMIT_1   constant varchar2(2) := '12' ;    -- COLFACT group count(groups: top, middle, bottom)
  C_COLFACT_LIMIT_2   constant varchar2(2) :=  '6' ;    -- should be C_COLFACT_LIMIT_1 / 2
  C_NL                constant char        := chr(10) ; -- new line character

  g_owner    DA_RUN.run_owner%TYPE ;
  g_run_key  DA_RUN.run_key%TYPE ;
  g_tab_key  DA_TAB.tab_key%TYPE ;
  g_col_key  DA_COL.col_key%TYPE ;

  procedure ins_COL( p_run_key      in integer,
                     p_tab_ins_date in date := null )
  as
  begin
    -- insert/update DA_COL (columns)
    merge into
      DA_COL m
    using
      ( with
        X_TAB
        as
          ( select
              a.RUN_KEY,
              b.TAB_KEY,
              a.RUN_OWNER       as OWNER,
              b.TAB_NAME        as TABLE_NAME,
              c.CONSTRAINT_NAME as PK_IND_NAME
            from
              DA_RUN a
              inner join DA_TAB b               on ( a.RUN_KEY = b.RUN_KEY
                                                   )
              left outer join ALL_CONSTRAINTS c on (     a.RUN_OWNER = c.OWNER
                                                     and b.TAB_NAME  = c.TABLE_NAME
                                                     and 'P'         = c.CONSTRAINT_TYPE
                                                   )
            where
              a.RUN_KEY = p_run_key
              and (p_tab_ins_date is null or b.INSERTED_DATE = p_tab_ins_date)
          )
        select
          e.TAB_KEY,
          f.COLUMN_NAME     as COL_NAME,
          f.COLUMN_ID       as COL_SEQNO,
          case when f.NULLABLE = 'N' then 'Y' end as COL_MANDATORY,
          h.COLUMN_POSITION as COL_PK,
          f.DATA_TYPE       as COL_DATA_TYPE,
          case f.DATA_TYPE
            when 'CHAR'      then 'C' -- char
            when 'VARCHAR'   then 'C'
            when 'VARCHAR2'  then 'C'
            when 'NUMBER'    then 'N' -- num
            when 'INTEGER'   then 'N'
            when 'FLOAT'     then 'N'
            when 'DATE'      then 'D' -- datetime
            when 'TIMESTAMP' then 'D'
            when 'BLOB'      then 'B' -- big
            when 'CLOB'      then 'B'
                             else 'x'
          end as COL_DATA_TYPE2,
          --
          f.DATA_LENGTH     as COL_DATA_LENGTH,
          f.DATA_PRECISION  as COL_DATA_PRECISION,
          f.DATA_SCALE      as COL_DATA_SCALE,
          g.COMMENTS        as COL_DESC
        from
          X_TAB e
          inner join ALL_TAB_COLUMNS f       on (     e.OWNER       = f.OWNER
                                                  and e.TABLE_NAME  = f.TABLE_NAME
                                                )
          left outer join ALL_COL_COMMENTS g on (     f.OWNER       = g.OWNER
                                                  and f.TABLE_NAME  = g.TABLE_NAME
                                                  and f.COLUMN_NAME = g.COLUMN_NAME
                                                )
          left outer join ALL_IND_COLUMNS h  on (     e.OWNER       = h.TABLE_OWNER
                                                  and e.TABLE_NAME  = h.TABLE_NAME
                                                  and e.PK_IND_NAME = h.INDEX_NAME
                                                  and f.COLUMN_NAME = h.COLUMN_NAME )
      ) s
    on
      (     m.TAB_KEY  = s.TAB_KEY
        and m.COL_NAME = s.COL_NAME
      )
    when matched then update set
      m.COL_MANDATORY      = s.COL_MANDATORY,
      m.COL_PK             = m.COL_PK,
      m.COL_SEQNO          = s.COL_SEQNO,
      m.COL_DATA_TYPE      = m.COL_DATA_TYPE,
      m.COL_DATA_TYPE2     = m.COL_DATA_TYPE2,
      m.COL_DATA_LENGTH    = m.COL_DATA_LENGTH,
      m.COL_DATA_PRECISION = m.COL_DATA_PRECISION,
      m.COL_DATA_SCALE     = m.COL_DATA_SCALE,
      m.COL_DESC           = m.COL_DESC,
      m.UPDATED_DATE       = sysdate
    when not matched then insert(
      COL_KEY, TAB_KEY, COL_NAME, COL_MANDATORY,
      COL_PK, COL_SEQNO, COL_DATA_TYPE, COL_DATA_TYPE2, COL_DATA_LENGTH,
      COL_DATA_PRECISION, COL_DATA_SCALE, COL_DESC, INSERTED_DATE )
    values(
      da_col_key_sq.NEXTVAL, s.TAB_KEY, s.COL_NAME, s.COL_MANDATORY,
      s.COL_PK, s.COL_SEQNO, s.COL_DATA_TYPE, s.COL_DATA_TYPE2, s.COL_DATA_LENGTH,
      s.COL_DATA_PRECISION, s.COL_DATA_SCALE, s.COL_DESC, sysdate ) ;

    dbms_output.PUT_LINE(to_char(SQL%ROWCOUNT)||' - rows merged into DA_COL.') ;

  end ins_COL ;


  procedure upd_COL_DEFAULT( p_run_key      in integer,
                             p_tab_ins_date in date := null )
  --
  -- update DA_COL.col_default (column default value if length <= 2000)
  is
    cursor c_dflt is
      select
        c.COL_KEY,
        d.DATA_DEFAULT -- LONG datatype
      from
        DA_RUN a
        inner join DA_TAB b          on ( a.RUN_KEY = b.RUN_KEY )
        inner join DA_COL c          on ( b.TAB_KEY = c.TAB_KEY
                                        )
        inner join ALL_TAB_COLUMNS d on (     a.RUN_OWNER = d.OWNER
                                          and b.TAB_NAME  = d.TABLE_NAME
                                          and c.COL_NAME  = d.COLUMN_NAME
                                        )
      where
            a.RUN_KEY = p_run_key
        and (p_tab_ins_date is null or b.INSERTED_DATE = p_tab_ins_date)
        and d.DEFAULT_LENGTH is not null
        and d.DEFAULT_LENGTH <= 2000 ;

    l_col_key_TA       t_TA_col_key ;
    l_data_default_TA  t_TA_col_default ;
    l_cnt              integer := 0 ;
  begin
    open c_dflt ;
    loop
      fetch c_dflt bulk collect into l_col_key_TA, l_data_default_TA limit 1024 ;

      if l_col_key_TA.COUNT > 0 then
        forall i in 1..l_col_key_TA.COUNT
          update DA_COL set COL_DEFAULT = l_data_default_TA(i) where COL_KEY = l_col_key_TA( i ) ;

        l_cnt := l_cnt + l_col_key_TA.COUNT ;
      end if ;

      exit when c_dflt%NOTFOUND ;
    end loop ;
    close c_dflt ;

    dbms_output.PUT_LINE(to_char(l_cnt)||' - rows updated for DA_COL.col_default.') ;

  end upd_COL_DEFAULT ;


  procedure ins_TAB( p_run_key in integer )
  as
    l_current_datetime  DATE := sysdate ;
  begin
    -- insert/update DA_TAB (tables)
    merge into
      DA_TAB m
    using
      ( select
          a.RUN_KEY,
          b.TABLE_NAME,
          substr( c.COMMENTS, 1, 2000 ) as TAB_DESC
        from
          DA_RUN a
          inner join ALL_TABLES b            on ( a.RUN_OWNER = b.OWNER
                                                )
          left outer join ALL_TAB_COMMENTS c on (     b.OWNER      = c.OWNER
                                                  and b.TABLE_NAME = c.TABLE_NAME
                                                )
        where
          a.RUN_KEY = p_run_key
          and
            (     ( a.RUN_MASK_LIKE    is null or b.TABLE_NAME like     a.RUN_MASK_LIKE )
              and ( a.RUN_MASK_NOTLIKE is null or b.TABLE_NAME not like a.RUN_MASK_NOTLIKE )
              and ( a.RUN_MASK_REGEXP_LIKE is null or regexp_like( b.TABLE_NAME, a.RUN_MASK_REGEXP_LIKE ) )
            )
      ) s
    on
      (     m.RUN_KEY  = s.RUN_KEY
        and m.TAB_NAME = s.TABLE_NAME
      )
    when matched then update set
      m.TAB_DESC     = s.TAB_DESC,
      m.UPDATED_DATE = l_current_datetime
    when not matched then insert(
      TAB_KEY,
      RUN_KEY, TAB_NAME, TAB_DESC,
      INSERTED_DATE )
    values(
      da_tab_key_sq.NEXTVAL,
      s.RUN_KEY, s.TABLE_NAME, s.TAB_DESC,
      l_current_datetime ) ;

    dbms_output.PUT_LINE(to_char(SQL%ROWCOUNT)||' - rows merged into DA_TAB.') ;

    -- insert into DA_COL (columns)
    ins_COL(         p_run_key, l_current_datetime ) ;
    -- update DA_COL.data_default (column default value)
    upd_COL_DEFAULT( p_run_key, l_current_datetime ) ;
  end ins_TAB ;


  procedure init( p_name             in varchar2,
                  p_owner            in varchar2 := null,
                  p_mask_like        in varchar2 := null,
                  p_mask_notlike     in varchar2 := null,
                  p_mask_regexp_like in varchar2 := null,
               -- p_enum_flag        in varchar2 := 'N',
               -- p_db_link          in varchar2 := null,
                  p_description      in varchar2 := null )
  is
  begin
    g_owner := nvl( p_owner, USER ) ;
    g_tab_key := null ;
    g_col_key := null ;

    --- insert into DA_RUN
    insert into DA_RUN(
      RUN_KEY,
      RUN_NAME,
      RUN_OWNER,
      RUN_MASK_LIKE,
      RUN_MASK_NOTLIKE,
      RUN_MASK_REGEXP_LIKE,
  --  RUN_ENUM_FLAG,
  --  RUN_DB_LINK,
      RUN_DESC,
      INSERTED_DATE )
    values(
      da_run_key_sq.NEXTVAL,
      p_name,
      g_owner,
      case when p_mask_regexp_like is null then p_mask_like    end,
      case when p_mask_regexp_like is null then p_mask_notlike end,
      p_mask_regexp_like,
   -- nvl( p_enum_flag,'N'),
   -- p_db_link,
      p_description,
      sysdate )
    returning RUN_KEY into g_run_key ;

    dbms_output.PUT_LINE('>> da_run.RUN_KEY = '||to_char( g_run_key )) ;

    -- print warnings
    if p_mask_regexp_like is not null then
      if p_mask_like is not null then
        dbms_output.PUT_LINE('Warning: like_mask ('||p_mask_like||') is ignored.') ;
      end if ;
      if p_mask_notlike is not null then
        dbms_output.PUT_LINE('Warning: not_like_mask ('||p_mask_notlike||') is ignored.') ;
      end if ;
    end if ;

    -- insert into DA_TAB (tables) and DA_COLS (columns)
    ins_TAB( g_run_key ) ;
    commit ;
  end init ;


  procedure del( p_run_key in integer )
  -- delete DA_RUN and all detail tables
  is
    l_run_name  DA_RUN.run_name%TYPE ;
  begin
    begin
      select a.RUN_NAME into l_run_name from DA_RUN a where a.RUN_KEY = p_run_key ;
    exception
      when NO_DATA_FOUND then
        dbms_output.PUT_LINE('No data found for RUN_KEY = '||nvl( to_char(p_run_key),'null')||'.') ;
    end ;

    if l_run_name is not null then
      -- delete from DA_LOG
      delete from DA_LOG where RUN_KEY = p_run_key ;
      dbms_output.PUT_LINE( to_char(SQL%ROWCOUNT)||' - rows deleted at DA_LOG (run_key).') ;
/* -- delete from DA_LOG
      where
        TAB_KEY in
        ( select a.TAB_KEY from DA_TAB a where a.RUN_KEY = p_run_key
        ) ;
      dbms_output.PUT_LINE( to_char(SQL%ROWCOUNT)||' - rows deleted at DA_LOG (tab_key).') ;

      delete from DA_LOG
      where
        COL_KEY in
        ( select a.COL_KEY
          from
            DA_COL a
            inner join DA_TAB b on ( a.TAB_KEY = b.TAB_KEY )
          where
            b.RUN_KEY = p_run_key
        ) ;
      dbms_output.PUT_LINE( to_char(SQL%ROWCOUNT)||' - rows deleted at DA_LOG (col_key).') ; -- */

      -- delete from DA_COLFACT:
      delete from DA_COLFACT
      where
        COL_KEY in
        ( select a.COL_KEY
          from
            DA_COL a
            inner join DA_TAB b on ( a.TAB_KEY = b.TAB_KEY )
          where
            b.RUN_KEY = p_run_key
        ) ;
      dbms_output.PUT_LINE( to_char(SQL%ROWCOUNT)||' - rows deleted at DA_COLFACT.') ;
      -- delete from DA_COL:
      delete from DA_COL
      where
        TAB_KEY in
        ( select a.TAB_KEY
          from   DA_TAB a
          where  a.RUN_KEY = p_run_key
        ) ;
      dbms_output.PUT_LINE( to_char(SQL%ROWCOUNT)||' - rows deleted at DA_COL.') ;
      -- delete from DA_TAB:
      delete from DA_TAB where RUN_KEY = p_run_key ;
      dbms_output.PUT_LINE( to_char(SQL%ROWCOUNT)||' - rows deleted at DA_TAB.') ;
      -- delete from DA_RUN:
      delete from DA_RUN where RUN_KEY = p_run_key ;
      dbms_output.PUT_LINE( to_char(SQL%ROWCOUNT)||' - rows deleted at DA_RUN.') ;
    end if ;

  end del ;


  procedure log( p_type    in varchar2, -- E: error, W: warning, I: info
                 p_msg     in varchar2,
                 p_clob    in varchar2 := null )
  is
    pragma autonomous_transaction ;
  begin
    insert into DA_LOG(
      LOG_TIME, LOG_TYPE, LOG_MSG, LOG_STACK, RUN_KEY, TAB_KEY, COL_KEY )
    values(
      sysdate, p_type, p_msg, p_clob, g_run_key, g_tab_key, g_col_key ) ;
    commit ;
  exception
    when OTHERS then
      rollback ; raise ;
  end log ;


  function get_tab_expr( p_table_name in varchar2,
                         p_alias      in varchar2 ) return varchar2
  is
  begin
    return lower( g_owner )||'.'||p_table_name||' '||p_alias ;
  end get_tab_expr ;


  procedure gen_tab_query( p_table_name     in     varchar2,
                           p_col_name_TA    in     t_TA_col_name,
                           p_col_type2_TA   in     t_TA_col_type2,
                           p_col_default_TA in     t_TA_col_default,
                           p_fce            in     pls_integer, -- 1: count, 2: count distinct
                           -- in/out parameters:
                           pa_query_text    in out varchar2,
                           pa_query_cols    in out pls_integer )
  is
    l_col_type   DA_COL.col_data_type2%TYPE ;
    l_col_name   ALL_TAB_COLUMNS.column_name%TYPE ;
    l_col_expr   varchar2(256) ;
  begin
    pa_query_text := 'select' ;
    for i in 1..p_col_name_TA.COUNT
    loop
      l_col_name := p_col_name_TA(  i ) ;
      l_col_type := p_col_type2_TA( i ) ;

      if p_fce = 1 then -- count -----------------------------------------------
        if l_col_type = 'B' then
          l_col_expr := 'count( case when a.'|| l_col_name ||' is not null then 1 end)' ;
        else
          l_col_expr := 'count(a.'|| l_col_name ||')' ;
        end if ;
      elsif p_fce = 2 then -- count distinct -----------------------------------
        if l_col_type = 'B' then
          l_col_expr := 'cast(null as number(2))' ;
        else
          l_col_expr := 'count(distinct a.'|| l_col_name ||')' ;
        end if ;
      elsif p_fce = 3 then -- no default count ---------------------------------
        if p_col_default_TA(i) is null or l_col_type = 'B' then
          l_col_expr := 'cast(null as number(2))' ;
        else
          l_col_expr := 'count(nullif(a.'|| l_col_name ||','|| p_col_default_TA(i) ||'))' ;
        end if ;
      elsif p_fce = 4 then -- char length (min,max) ----------------------------
        if l_col_type != 'C' then
          l_col_expr := 'cast(null as number(2)) as A'||to_char(i)||
                      ', cast(null as number(2))' ;
        else
          l_col_expr := 'min(length(a.'|| l_col_name ||')) as A'||to_char(i)||
                      ', max(length(a.'|| l_col_name ||'))' ;
        end if ;
        ------------------------------------------------------------------------
      end if ;
      pa_query_text := pa_query_text || case when i > 1 then ',' end || C_NL ||
                       l_col_expr ||' as N'||to_char(i) ;
    end loop ;

    -- set pa_query_col (columns count at pa_query_text)
    if p_fce = 1 then
      pa_query_text := pa_query_text ||', count(1) as N' ; -- count(*) added
      pa_query_cols := p_col_name_TA.COUNT + 1 ;
    elsif p_fce in (2,3) then
      pa_query_cols := p_col_name_TA.COUNT ;
    elsif p_fce = 4 then
      pa_query_cols := p_col_name_TA.COUNT * 2 ;
    end if ;

    -- from clause
    pa_query_text := pa_query_text || C_NL ||'from'|| C_NL || get_tab_expr( p_table_name,'a') ;
  end gen_tab_query ;


  procedure run_query( p_query_text  in     varchar2,
                       p_query_cols  in     pls_integer,
                       p_col_name_TA in     t_TA_col_name,
                       pa_col_cnt_TA in out t_TA_col_cnt,
                       pa_cnt_all    in out integer )
  is
    l_cursor     sys_refcursor ;
    l_cursor_no  integer ;
    l_value      integer ;
  begin
    open l_cursor for p_query_text ;
    l_cursor_no := DBMS_SQL.to_cursor_number( l_cursor ) ;

    for i in 1..p_query_cols
    loop
      DBMS_SQL.define_column( l_cursor_no, i, 1 ) ; -- 1 means number (number example)
    end loop ;

    if DBMS_SQL.fetch_rows( l_cursor_no ) > 0 then
      for i in 1..p_query_cols
      loop
        DBMS_SQL.column_value( l_cursor_no, i, l_value ) ;
        if i <= p_col_name_TA.COUNT then
          pa_col_cnt_TA( i ) := l_value ;
        else
          pa_cnt_all := l_value ;
        end if ;
      end loop ;
    end if ;

    DBMS_SQL.close_cursor( l_cursor_no ) ;
  exception
    when OTHERS then
      log( p_type => 'E',
           p_msg  => SQLERRM,
           p_clob => p_query_text ) ;
      raise ;
  end run_query ;


  procedure run_query2( p_query_text   in     varchar2,
                        p_query_cols   in     pls_integer,
                        p_col_name_TA  in     t_TA_col_name,
                        pa_col_len1_TA in out t_TA_col_len,  -- odd cols
                        pa_col_len2_TA in out t_TA_col_len   -- even cols
                      )
  is
    l_cursor     sys_refcursor ;
    l_cursor_no  integer ;
    l_value      integer ;
    l_odd_fb     boolean        := true ;
    l_ix         simple_integer := 0 ;
  begin
    open l_cursor for p_query_text ;
    l_cursor_no := DBMS_SQL.to_cursor_number( l_cursor ) ;

    for i in 1..p_query_cols
    loop
      DBMS_SQL.define_column( l_cursor_no, i, 1 ) ; -- 1 means number (number example)
    end loop ;

    if DBMS_SQL.fetch_rows( l_cursor_no ) > 0 then
      for i in 1..p_query_cols
      loop
        DBMS_SQL.column_value( l_cursor_no, i, l_value ) ;
        if l_odd_fb then
          l_ix := l_ix + 1 ;
          pa_col_len1_TA( l_ix ) := l_value ;
          l_odd_fb := false ;
        else
          pa_col_len2_TA( l_ix ) := l_value ;
          l_odd_fb := true ;
        end if ;
      end loop ;
    end if ;

    DBMS_SQL.close_cursor( l_cursor_no ) ;
  exception
    when OTHERS then
      log( p_type => 'E',
           p_msg  => SQLERRM,
           p_clob => p_query_text ) ;
      raise ;
  end run_query2 ;


  function sp( p_len in integer ) return varchar2
  -- spaces string with length of p_len
  is
  begin
    return rpad(' ', p_len ) ;
  end sp ;


  procedure colfact_insert( p_col_key     in integer,
                            p_tab_name    in varchar2,
                            p_col_name    in varchar2,
                            p_col_type2   in varchar2,
                            p_col_len_db  in integer,
                            p_col_metric  in varchar2,
                            pa_query_text in out varchar2 )
  --
  -- insert into DA_COLFACT - values
  is
    l_col_expr_1  varchar2(32) ;
    l_col_expr_2  varchar2(64) ;
  begin
    g_col_key := p_col_key ;

    l_col_expr_1 := 'a.'||p_col_name ;
    if p_col_type2 = 'C' and p_col_len_db > C_COLFACT_VALUE_MAX then
      l_col_expr_2 := 'substr('||l_col_expr_1||',1,'||to_number( C_COLFACT_VALUE_MAX-3)||')||''...''' ;
    else
      l_col_expr_2 := l_col_expr_1 ;
    end if ;

    -- generate insert/select command
    pa_query_text := q'{insert into DA_COLFACT(
  COLFACT_KEY,
  COL_KEY,
  COLFACT_METRIC,
  COLFACT_ORDER,
  COLFACT_COUNT,
  COLFACT_VALUE,
  INSERTED_DATE )
with
X_SET
as
  ( select
}'||
sp(6)||l_col_expr_2||' as VAL,'||C_NL||
sp(6)||'count('||l_col_expr_1||') as CNT,'||C_NL||
sp(6)||'row_number() over(order by '||
case when p_col_metric='F' then 'count('||l_col_expr_1||'), ' end||
l_col_expr_2||') as ORD,'||C_NL||
sp(6)||'count('||l_col_expr_2||') over () as CNT_ALL'||C_NL||
sp(4)||'from'||C_NL||
sp(6)||get_tab_expr( p_tab_name,'a')||C_NL||
sp(4)||'where'||C_NL||
sp(6)||l_col_expr_1||' is not null'||C_NL||
sp(4)||'group by'||C_NL||
sp(6)||l_col_expr_2||C_NL||
sp(2)||')'||C_NL||
'select'||C_NL||
sp(2)||'da_colfact_key_sq.NEXTVAL as COLFACT_KEY,'||C_NL||
sp(2)||to_char(p_col_key)||' as COL_KEY,'||C_NL||
sp(2)||''''||p_col_metric||''' as METRIC,'||C_NL||
sp(2)||'b.ORD,'||C_NL||
sp(2)||'b.CNT,'||C_NL||
sp(2)||case p_col_type2
         when 'N' then 'to_char(b.VAL) as VAL'
         when 'D' then q'{to_char(b.VAL,'YYYY-MM-DD HH24:MI:SS') as VAL}'
                  else 'b.VAL'
       end||q'{,
  sysdate as INSERTED_DATE
from
  X_SET b
where
     b.ORD <= }'||
C_COLFACT_LIMIT_1 || C_NL ||
sp(2)||'or b.ORD between floor(b.CNT_ALL/2) - '|| C_COLFACT_LIMIT_2 ||
                    ' and ceil(b.CNT_ALL/2) + '|| C_COLFACT_LIMIT_2 ||C_NL||
sp(2)||'or b.ORD > (b.CNT_ALL -'|| C_COLFACT_LIMIT_1 ||')' ;

    -- run command
    begin
      execute immediate pa_query_text ;
  --  log('I','SQL', pa_query_text ) ;
    exception
      when OTHERS then
        log('E', SQLERRM, pa_query_text ) ;
        raise ;
    end ;
    g_col_key := null ;

  end colfact_insert ;


  procedure col_calc( p_tab_key         in integer,
                      p_tab_name        in varchar2,
                      p_col_regexp_like in varchar2 := null )
  --
  -- update DA_COL( COL_CNT_ALL, COL_CNT_DISTINCT, COL_CNT_NO_DEFAULT, COL_LEN_MIN, COL_LEN_MAX
  -- delete/insert DA_COLFACT
  is
    -- input arrays:
    l_col_key_TA            t_TA_col_key ;
    l_col_name_TA           t_TA_col_name ;
    l_col_type2_TA          t_TA_col_type2 ;
    l_col_len_db_TA         t_TA_col_len_db ;
    l_col_default_TA        t_TA_col_default ;
    -- output arrays:
    l_col_cnt_all_TA        t_TA_col_cnt ;
    l_col_cnt_distinct_TA   t_TA_col_cnt ;
    l_col_cnt_no_default_TA t_TA_col_cnt ;
    l_col_len_min_TA        t_TA_col_len ;
    l_col_len_max_TA        t_TA_col_len ;
    -- scalar variables:
    l_query_text            t_varchar2_max ;
    l_query_cols            pls_integer ;
    l_cnt_all               DA_TAB.tab_num_rows%TYPE ;
    l_calc_time_start       date := sysdate ;

    function col_default_NOT_NUL_exists( p_col_default_TA in t_TA_col_default ) return boolean
    is
      l_exists_fb  boolean := false ;
    begin
      for i in 1..p_col_default_TA.COUNT
      loop
        if p_col_default_TA(i) is not null then
          l_exists_fb := true ; exit ;
        end if ;
      end loop ;
      return l_exists_fb ;
    end col_default_NOT_NUL_exists ;

    function col_type2_CHAR_exists( p_col_type2_TA in t_TA_col_type2 ) return boolean
    is
      l_exists_fb  boolean := false ;
    begin
      for i in 1..p_col_type2_TA.COUNT
      loop
        if p_col_type2_TA(i) = 'C' then
          l_exists_fb := true ; exit ;
        end if ;
      end loop ;
      return l_exists_fb ;
    end col_type2_CHAR_exists ;
  begin
    -- read DA_COL records for TAB_KEY
    select
      a.COL_KEY, a.COL_NAME, a.COL_DATA_TYPE2, a.COL_DATA_LENGTH, a.COL_DEFAULT,
      null, null, null, null, null
    bulk collect into
      l_col_key_TA, l_col_name_TA, l_col_type2_TA, l_col_len_db_TA, l_col_default_TA,
      -- alocate and initialize output arrays (NULL) only:
      l_col_cnt_all_TA, l_col_cnt_distinct_TA, l_col_cnt_no_default_TA, l_col_len_min_TA, l_col_len_max_TA
    from
      DA_COL a
    where
      a.TAB_KEY = p_tab_key
      and (p_col_regexp_like is null or regexp_like( a.COL_NAME, p_col_regexp_like ))
    order by
      a.COL_SEQNO ;

    -- generate and run queries
    if l_col_key_TA.COUNT > 0 then
      -- 1: count
      gen_tab_query( p_table_name     => p_tab_name,
                     p_col_name_TA    => l_col_name_TA,
                     p_col_type2_TA   => l_col_type2_TA,
                     p_col_default_TA => l_col_default_TA, -- not used for fce=1
                     p_fce            => 1, -- count
                     pa_query_text    => l_query_text,
                     pa_query_cols    => l_query_cols ) ;

      run_query( p_query_text  => l_query_text,
                 p_query_cols  => l_query_cols,
                 p_col_name_TA => l_col_name_TA,
                 pa_col_cnt_TA => l_col_cnt_all_TA,
                 pa_cnt_all    => l_cnt_all ) ;

      -- 2: count distinct
      gen_tab_query( p_table_name     => p_tab_name,
                     p_col_name_TA    => l_col_name_TA,
                     p_col_type2_TA   => l_col_type2_TA,
                     p_col_default_TA => l_col_default_TA, -- not used for fce=2
                     p_fce            => 2, -- count distinct
                     pa_query_text    => l_query_text,
                     pa_query_cols    => l_query_cols ) ;

      run_query( p_query_text  => l_query_text,
                 p_query_cols  => l_query_cols,
                 p_col_name_TA => l_col_name_TA,
                 pa_col_cnt_TA => l_col_cnt_distinct_TA,
                 pa_cnt_all    => l_cnt_all -- not used for fce=2
               ) ;

      if col_default_NOT_NUL_exists( l_col_default_TA ) then
        -- 3: no default count
        gen_tab_query( p_table_name     => p_tab_name,
                       p_col_name_TA    => l_col_name_TA,
                       p_col_type2_TA   => l_col_type2_TA,
                       p_col_default_TA => l_col_default_TA,
                       p_fce            => 3, -- no default count
                       pa_query_text    => l_query_text,
                       pa_query_cols    => l_query_cols ) ;

        run_query( p_query_text  => l_query_text,
                   p_query_cols  => l_query_cols,
                   p_col_name_TA => l_col_name_TA,
                   pa_col_cnt_TA => l_col_cnt_no_default_TA,
                   pa_cnt_all    => l_cnt_all -- not used for fce=3
                 ) ;
      end if ;

      if col_type2_CHAR_exists( l_col_type2_TA ) then
        -- 4: character columns length, min/max
        gen_tab_query( p_table_name     => p_tab_name,
                       p_col_name_TA    => l_col_name_TA,
                       p_col_type2_TA   => l_col_type2_TA,
                       p_col_default_TA => l_col_default_TA,
                       p_fce            => 4, -- character columns length, min/max
                       pa_query_text    => l_query_text,
                       pa_query_cols    => l_query_cols ) ;

        run_query2( p_query_text   => l_query_text,
                    p_query_cols   => l_query_cols,
                    p_col_name_TA  => l_col_name_TA,
                    pa_col_len1_TA => l_col_len_min_TA, -- min length
                    pa_col_len2_TA => l_col_len_max_TA  -- max length
                  ) ;
      end if ;

      -- update DA_COL
      forall i in 1..l_col_key_TA.COUNT
        update DA_COL set
          COL_CNT_ALL        = l_col_cnt_all_TA(i),
          COL_CNT_DISTINCT   = l_col_cnt_distinct_TA(i),
          COL_CNT_NO_DEFAULT = l_col_cnt_no_default_TA(i),
          COL_LEN_MIN        = l_col_len_min_TA(i),
          COL_LEN_MAX        = l_col_len_max_TA(i),
          UPDATED_DATE       = sysdate
        where
          COL_KEY = l_col_key_TA( i ) ;

      -- delete/insert DA_COLFACT
      for i in 1..l_col_key_TA.COUNT
      loop
        if l_col_type2_TA(i) != 'B' then
          delete from DA_COLFACT where COL_KEY = l_col_key_TA(i) ;
          if l_col_cnt_all_TA(i) > 0 then
            colfact_insert( p_col_key     => l_col_key_TA(i),
                            p_tab_name    => p_tab_name,
                            p_col_name    => l_col_name_TA(i),
                            p_col_type2   => l_col_type2_TA(i),
                            p_col_len_db  => l_col_len_db_TA(i),
                            p_col_metric  => 'V', -- value
                            pa_query_text => l_query_text ) ;

            colfact_insert( p_col_key     => l_col_key_TA(i),
                            p_tab_name    => p_tab_name,
                            p_col_name    => l_col_name_TA(i),
                            p_col_type2   => l_col_type2_TA(i),
                            p_col_len_db  => l_col_len_db_TA(i),
                            p_col_metric  => 'F', -- frequency
                            pa_query_text => l_query_text ) ;
          end if ;
        end if ;
      end loop ;

      -- update DA_TAB
      update DA_TAB
      set
        TAB_NUM_ROWS        = l_cnt_all,
        TAB_CALC_TIME_START = l_calc_time_start,
        TAB_CALC_TIME_END   = sysdate
      where
        TAB_KEY = p_tab_key ;

      commit ;

  /*  dbms_output.PUT_LINE('cnt_all = '||to_char(l_cnt_all)) ;
      for i in 1..l_col_name_TA.COUNT
      loop
        dbms_output.PUT_LINE( l_col_name_TA(i)||': '||
                              to_char(l_col_cnt_all_TA(i))||', '||
                              to_char(l_col_cnt_distinct_TA(i))||', '||
                              to_char(l_col_cnt_no_default_TA(i)) ) ;
      end loop ; */
    end if ;
  end col_calc ;


  procedure go( p_run_key         in integer,
                p_tab_regexp_like in varchar2 := null,
                p_col_regexp_like in varchar2 := null )
  is
    cursor c_tab is
      select a.TAB_KEY
      from
        DA_TAB a
      where
        a.RUN_KEY = p_run_key
        and (p_tab_regexp_like is null or regexp_like( a.TAB_NAME, p_tab_regexp_like ) )
      order by
        a.TAB_NAME ;
    
    l_tab_key_TA  t_TA_tab_key ;
    l_tab_name    DA_TAB.tab_name%TYPE ;
  begin
    select a.RUN_KEY, a.RUN_OWNER into g_run_key, g_owner
    from   DA_RUN a
    where  a.RUN_KEY = p_run_key ;

    open c_tab ; fetch c_tab bulk collect into l_tab_key_TA ; close c_tab ;

    for i in 1..l_tab_key_TA.COUNT
    loop
      g_tab_key := l_tab_key_TA( i ) ;
      select a.TAB_NAME into l_tab_name from DA_TAB a where a.TAB_KEY = g_tab_key ;

      dbms_output.PUT_LINE('>> '||l_tab_name ) ;
      col_calc( g_tab_key, l_tab_name, p_col_regexp_like ) ;
    end loop ;
  end go ;


  function get_job_name( p_run_key in integer ) return varchar2
  is
    l_run_key  DA_RUN.run_key%TYPE ;
  begin
    -- existence test for p_run_key only.
    select a.RUN_KEY into l_run_key from DA_RUN a where a.RUN_KEY = p_run_key ;

    -- return job_name
    return 'DOM_AN_'||to_char( p_run_key ) ;
  exception
    when NO_DATA_FOUND then
      dbms_output.PUT_LINE('Unknown RUN_KEY = '||nvl( to_char(p_run_key),'null')||'.') ;
      return null ;

  end get_job_name ;


  procedure go_job_start( p_run_key in integer )
  is
    l_job_name  varchar2(30) ;
  begin
    l_job_name := get_job_name( p_run_key ) ;
    if l_job_name is not null then

      DBMS_SCHEDULER.create_job(
        job_name   =>  l_job_name,
        job_type   => 'PLSQL_BLOCK',
        job_action => 'begin da_proc_p.GO('|| to_char(p_run_key) ||') ; end ;',
        enabled    =>  TRUE,
        comments   => 'Domain Analysis (DA_RUN.run_key='|| to_char(p_run_key) ||').'
        ) ;

      dbms_output.PUT_LINE('Job '||l_job_name||' created.') ;
    end if ;
  end go_job_start ;


  procedure go_job_stop( p_run_key in integer )
  is
    l_job_name  varchar2(30) ;
  begin
    l_job_name := get_job_name( p_run_key ) ;
    if l_job_name is not null then
      DBMS_SCHEDULER.stop_job( l_job_name ) ;
      dbms_output.PUT_LINE('Job '||l_job_name||' stopped.') ;
    end if ;
  end go_job_stop ;

end DA_PROC_P ;
/
show errors
