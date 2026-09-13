-- ================================================================
-- RLS Policy Migration
-- Supabase security advisor: rls_enabled_no_policy (14 tables) → 해소 완료
-- Supabase security advisor: rls_policy_always_true (21 tables) → 본 개정으로 해소
--
-- 목적: RLS 활성화된 테이블에 backend_all 정책을 "service_role 전용"으로 추가/보정.
--   이 앱은 asyncpg 직접 연결(Supabase pooler의 postgres 슈퍼유저)만 사용하며
--   슈퍼유저는 RLS를 완전히 우회하므로 정책의 TO 절은 앱 동작에 영향을 주지 않는다.
--   그러나 정책에 역할 제한이 없으면(기본 PUBLIC) PostgREST의 anon/authenticated
--   롤도 USING(true)/WITH CHECK(true)로 전체 테이블에 접근 가능해지므로,
--   TO service_role로 명시해 anon/authenticated 접근을 실제로 차단한다.
--
-- 실행: pgAdmin 또는 Supabase SQL 에디터에서 전체 선택 후 실행.
-- 멱등: 정책이 없으면 생성(TO service_role), 있으면 ALTER POLICY로 역할만 보정.
-- ================================================================

DO $$ DECLARE
  _tbl TEXT;
  _tables TEXT[] := ARRAY[
    'news_articles', 'trade_signals', 'daily_ohlcv', 'daily_flow',
    'chart_signals', 'stage_classifications', 'watchlist_vol_log',
    'intraday_volumes', 'krx_listings', 'ticker_names', 'trade_log',
    'scheduler_triggers', 'aftermarket_snap', 'paper_positions',
    'manual_portfolio', 'dart_extractions', 'apscheduler_jobs',
    'daily_market_snap', 'dart_companies', 'dart_disclosures',
    'dart_xbrl', 'dart_segments', 'dart_fundamentals', 'sector_daily_stats',
    'youtube_mention_raw', 'youtube_attention_scores', 'youtube_mention_forward_returns',
    'youtube_backfill_queue', 'paper_positions_archive_gen1',
    'paper_halt_watch'
  ];
BEGIN
  FOREACH _tbl IN ARRAY _tables LOOP
    -- 테이블이 존재하는 경우에만 처리
    IF EXISTS (
      SELECT FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = _tbl
    ) THEN
      -- RLS 활성화 (이미 활성화된 경우 무시됨)
      EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', _tbl);

      IF EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public' AND tablename = _tbl AND policyname = 'backend_all'
      ) THEN
        -- 기존 정책: 역할만 service_role로 보정 (rls_policy_always_true 경고 해소)
        EXECUTE format('ALTER POLICY backend_all ON public.%I TO service_role', _tbl);
        RAISE NOTICE '% : backend_all policy scoped to service_role', _tbl;
      ELSE
        -- 신규 정책: 처음부터 service_role 전용으로 생성
        EXECUTE format(
          'CREATE POLICY backend_all ON public.%I FOR ALL TO service_role USING (true) WITH CHECK (true)',
          _tbl
        );
        RAISE NOTICE '% : backend_all policy created (service_role only)', _tbl;
      END IF;
    ELSE
      RAISE NOTICE '% : table does not exist, skipped', _tbl;
    END IF;
  END LOOP;
END $$;
