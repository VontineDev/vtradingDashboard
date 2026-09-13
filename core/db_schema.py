"""
db_schema.py  —  테이블 DDL + init_db
────────────────────────────────────────────────────────────
전체 DDL 상수, RLS 활성화 목록, init_db(pool). core.db facade를 통해 re-export됨.
"""

from __future__ import annotations

import logging

import asyncpg

logger = logging.getLogger(__name__)


# ── 테이블 DDL ────────────────────────────────────────────────
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS news_articles (
    id           BIGSERIAL    PRIMARY KEY,
    url_hash     CHAR(16)     NOT NULL UNIQUE,
    url          TEXT         NOT NULL,
    source       VARCHAR(32)  NOT NULL,
    category     VARCHAR(32)  NOT NULL,
    title_en     TEXT         NOT NULL,
    summary_en   TEXT,
    summary_ko   TEXT,
    llm_backend  VARCHAR(16),
    published_at TIMESTAMPTZ,
    fetched_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_news_cat     ON news_articles (category);
CREATE INDEX IF NOT EXISTS idx_news_src     ON news_articles (source);
CREATE INDEX IF NOT EXISTS idx_news_pub     ON news_articles (published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_news_fetched ON news_articles (fetched_at  DESC);

CREATE TABLE IF NOT EXISTS trade_signals (
    id               BIGSERIAL    PRIMARY KEY,
    article_id       BIGINT       REFERENCES news_articles(id) ON DELETE CASCADE,
    direction        VARCHAR(8)   NOT NULL,
    strength         SMALLINT     NOT NULL,
    reason           TEXT,
    tickers          TEXT[],
    llm_backend      VARCHAR(16),
    macro_usd_krw    FLOAT,
    macro_base_rate  FLOAT,
    detected_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sig_direction ON trade_signals (direction);
CREATE INDEX IF NOT EXISTS idx_sig_detected  ON trade_signals (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_sig_strength  ON trade_signals (strength DESC);
-- Idempotent migration: add macro columns to existing deployments
ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS macro_usd_krw   FLOAT;
ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS macro_base_rate FLOAT;
-- Idempotent migration: add article_type classification
ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS article_type VARCHAR(20) DEFAULT 'other';

-- ── 일봉 OHLCV (1년치 히스토리) ─────────────────────────────
CREATE TABLE IF NOT EXISTS daily_ohlcv (
    id              BIGSERIAL       PRIMARY KEY,
    symbol          VARCHAR(32)     NOT NULL,
    market          VARCHAR(4)      NOT NULL,      -- 'US' | 'KR' | 'IDX' | 'CMD'
    date            DATE            NOT NULL,
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT           NOT NULL,
    volume          BIGINT,
    source          VARCHAR(16)     NOT NULL,       -- 'yfinance'
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_daily_sym_date
    ON daily_ohlcv (symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_market
    ON daily_ohlcv (market, date DESC);

-- ── 일별 외국인/기관 순매수 (3단계 스크리닝용) ─────────────
CREATE TABLE IF NOT EXISTS daily_flow (
    ticker           TEXT        NOT NULL,
    trade_date       DATE        NOT NULL,
    foreign_net      BIGINT,                  -- 외국인 순매수 (주)
    inst_net         BIGINT,                  -- 기관 순매수 (주)
    foreign_streak   SMALLINT,               -- 외국인 연속 순매수일 (음수 = 순매도)
    inst_streak      SMALLINT,               -- 기관 연속 순매수일 (음수 = 순매도)
    personal_net     BIGINT,                  -- 개인 순매수 (주). 음수 = 개인 순매도
    personal_streak  SMALLINT,               -- 개인 연속 순매수일
    created_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_daily_flow_date ON daily_flow (trade_date DESC);

-- ── 주봉 차트 스크리닝 결과 ──────────────────────────────────
CREATE TABLE IF NOT EXISTS chart_signals (
    id          SERIAL PRIMARY KEY,
    ticker      VARCHAR(20)  NOT NULL,
    name        VARCHAR(100),
    close       FLOAT,
    ma_20w      FLOAT,
    ma_60w      FLOAT,
    cloud_top   FLOAT,
    is_enhanced BOOLEAN      DEFAULT FALSE,
    has_gapjum  BOOLEAN      DEFAULT FALSE,
    week_of     VARCHAR(10)  NOT NULL,
    screened_at TIMESTAMPTZ  DEFAULT NOW(),
    sector      VARCHAR(80)  DEFAULT '',
    ma_120w     FLOAT,
    UNIQUE(ticker, week_of)
);
CREATE INDEX IF NOT EXISTS idx_chart_signals_week_ticker ON chart_signals(week_of, ticker);

-- ── 3단계 분류 결과 (일별, 전 종목 대상) ────────────────────
CREATE TABLE IF NOT EXISTS stage_classifications (
    ticker          TEXT        NOT NULL,
    classified_date DATE        NOT NULL,
    stage           SMALLINT    NOT NULL,  -- 1, 2, 3
    s1_entry_date   DATE,                  -- Stage 1 감지일
    s1_high         NUMERIC,               -- Stage 1 당일 고가
    s1_volume       BIGINT,                -- Stage 1 당일 거래량 (구버전 호환용, 신규는 s1_txamt 사용)
    s1_txamt        BIGINT,                -- Stage 1 당일 거래대금 = Volume × Close (원)
    peakout_flag    BOOLEAN     DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ticker, classified_date)
);
CREATE INDEX IF NOT EXISTS idx_stage_class_date ON stage_classifications (classified_date DESC);
CREATE INDEX IF NOT EXISTS idx_stage_class_ticker ON stage_classifications (ticker, classified_date DESC);

-- ── 워치리스트 일별 거래대금 비율 로그 (rally death / vol delta용) ──
CREATE TABLE IF NOT EXISTS watchlist_vol_log (
    ticker          TEXT        NOT NULL,
    trade_date      DATE        NOT NULL,
    vol_ratio       FLOAT,                   -- today_txamt / s1_txamt (거래대금 비율)
    s1_vol          BIGINT,                  -- Stage 1 진입 거래량 (구버전 호환용)
    s1_txamt        BIGINT,                  -- Stage 1 진입 거래대금 = Volume × Close (원)
    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_watchlist_vol_ticker ON watchlist_vol_log (ticker, trade_date DESC);

-- ── 분봉 거래량 데이터 (StockData.org / yfinance) ────────────
CREATE TABLE IF NOT EXISTS intraday_volumes (
    id              BIGSERIAL       PRIMARY KEY,
    symbol          VARCHAR(32)     NOT NULL,
    market          VARCHAR(4)      NOT NULL,      -- 'US' | 'KR'
    ts              TIMESTAMPTZ     NOT NULL,       -- 캔들 시작 시각 (UTC)
    interval        VARCHAR(8)      NOT NULL,       -- '1m' | '5m'
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    volume          BIGINT          NOT NULL,
    is_extended     BOOLEAN         NOT NULL DEFAULT FALSE,
    source          VARCHAR(16)     NOT NULL,       -- 'stockdata' | 'yfinance'
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (symbol, ts, interval)
);
CREATE INDEX IF NOT EXISTS idx_intraday_sym_ts
    ON intraday_volumes (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_market
    ON intraday_volumes (market, ts DESC);
"""

_CREATE_TRADE_LOG = """
CREATE TABLE IF NOT EXISTS trade_log (
    id              SERIAL          PRIMARY KEY,
    ticker          VARCHAR(12)     NOT NULL,
    entry_date      DATE            NOT NULL,
    entry_price     NUMERIC(12, 0)  NOT NULL,
    qty             INTEGER         NOT NULL,
    exit_date       DATE,
    exit_price      NUMERIC(12, 0),
    signal_date     DATE,
    stage_at_entry  SMALLINT,
    stage_at_exit   SMALLINT,
    entry_delay_days INTEGER GENERATED ALWAYS AS
                        ((entry_date - signal_date)) STORED,
    pnl             NUMERIC(14, 0) GENERATED ALWAYS AS
                        (CASE WHEN exit_price IS NOT NULL
                         THEN (exit_price - entry_price) * qty ELSE NULL END) STORED,
    pnl_pct         NUMERIC(7, 3) GENERATED ALWAYS AS
                        (CASE WHEN exit_price IS NOT NULL AND entry_price > 0
                         THEN ROUND((exit_price::numeric / entry_price - 1) * 100, 3)
                         ELSE NULL END) STORED,
    after_close_at_signal   NUMERIC(12, 0),
    after_chg_pct_at_signal NUMERIC(6, 2),
    memo            TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trade_log_ticker
    ON trade_log (ticker, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_log_stage
    ON trade_log (stage_at_entry, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_log_open
    ON trade_log (exit_date) WHERE exit_date IS NULL;
"""

_CREATE_SCHEDULER_TRIGGERS = """
CREATE TABLE IF NOT EXISTS scheduler_triggers (
    id           SERIAL       PRIMARY KEY,
    job_name     VARCHAR(50)  NOT NULL,
    requested_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    executed_at  TIMESTAMPTZ,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending'
);
CREATE INDEX IF NOT EXISTS idx_sched_trig_status
    ON scheduler_triggers (status, requested_at ASC)
    WHERE status = 'pending';
"""

_CREATE_KRX_TABLE = """
CREATE TABLE IF NOT EXISTS krx_listings (
    isin_code       TEXT PRIMARY KEY,
    short_code      TEXT NOT NULL,
    name_ko         TEXT NOT NULL,
    name_ko_abbr    TEXT,
    name_en         TEXT,
    listed_at       DATE,
    market          TEXT,
    security_type   TEXT,
    sector          TEXT,
    stock_type      TEXT,
    par_value       TEXT,
    listed_shares   BIGINT,
    yfinance_symbol TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_krx_listings_name_ko
    ON krx_listings (name_ko);
CREATE INDEX IF NOT EXISTS idx_krx_listings_name_ko_abbr
    ON krx_listings (name_ko_abbr);
CREATE INDEX IF NOT EXISTS idx_krx_listings_short_code
    ON krx_listings (short_code);
CREATE INDEX IF NOT EXISTS idx_krx_listings_updated_at
    ON krx_listings (updated_at);
"""

_CREATE_TICKER_NAMES = """
CREATE TABLE IF NOT EXISTS ticker_names (
    ticker      TEXT PRIMARY KEY,   -- yfinance 심볼: '005930.KS'
    name_ko     TEXT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_DART_TABLES = """
CREATE TABLE IF NOT EXISTS dart_companies (
    corp_code   VARCHAR(8)   PRIMARY KEY,  -- DART 8자리 기업고유번호
    corp_name   TEXT         NOT NULL,
    stock_code  VARCHAR(6),               -- KRX 6자리 종목코드 (상장사만)
    market      TEXT,                     -- KOSPI | KOSDAQ | NULL
    updated_at  TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dart_companies_stock_code
    ON dart_companies (stock_code)
    WHERE stock_code IS NOT NULL;

CREATE TABLE IF NOT EXISTS dart_disclosures (
    rcept_no    VARCHAR(14)  PRIMARY KEY,   -- 공시 접수번호 (14자리)
    corp_code   VARCHAR(8)   NOT NULL REFERENCES dart_companies(corp_code),
    corp_name   TEXT,
    report_nm   TEXT,                       -- 공시 제목
    rcept_dt    DATE,                       -- 접수일
    pblntf_ty   VARCHAR(4),                -- 공시유형 코드
    rm          TEXT,                       -- 비고 (유상증자 등 키워드)
    fetched_at  TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dart_disc_corp_dt
    ON dart_disclosures (corp_code, rcept_dt DESC);
CREATE INDEX IF NOT EXISTS idx_dart_disc_rcept_dt
    ON dart_disclosures (rcept_dt DESC);

CREATE TABLE IF NOT EXISTS dart_xbrl (
    id          BIGSERIAL    PRIMARY KEY,
    corp_code   VARCHAR(8)   NOT NULL REFERENCES dart_companies(corp_code),
    bsns_year   VARCHAR(4)   NOT NULL,   -- 사업연도 'YYYY'
    reprt_code  VARCHAR(5)   NOT NULL,   -- 11011=사업보고서, 11012=반기, 11013=1Q, 11014=3Q
    account_nm  TEXT         NOT NULL,
    fs_div      VARCHAR(4)   NOT NULL,   -- CFS=연결, OFS=별도
    amount      BIGINT,
    currency    VARCHAR(3)   DEFAULT 'KRW',
    fetched_at  TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (corp_code, bsns_year, reprt_code, account_nm, fs_div)
);
CREATE INDEX IF NOT EXISTS idx_dart_xbrl_corp_year
    ON dart_xbrl (corp_code, bsns_year DESC);

CREATE TABLE IF NOT EXISTS dart_segments (
    id          BIGSERIAL    PRIMARY KEY,
    corp_code   VARCHAR(8)   NOT NULL REFERENCES dart_companies(corp_code),
    bsns_year   VARCHAR(4)   NOT NULL,
    section     VARCHAR(10)  NOT NULL,   -- 'II-2' | 'II-4'
    raw_text    TEXT,                    -- Ollama 입력 원본
    parsed_json JSONB,                   -- Ollama 파싱 결과
    parse_ok    BOOLEAN      DEFAULT FALSE,
    model       TEXT,                    -- 사용 모델명
    fetched_at  TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (corp_code, bsns_year, section)
);
CREATE INDEX IF NOT EXISTS idx_dart_segments_corp_year
    ON dart_segments (corp_code, bsns_year DESC);

-- 전체시장 핵심 재무 스냅샷 (2026-08-06: TechnicalQuant.md 펀더멘털 스크리닝용).
-- dart_xbrl(계정과목 원장, 내러티브 기능 전용)과 별개 — fnlttSinglAcntAll 응답에서
-- 재무상태표/손익계산서 5개 핵심 계정만 뽑아 회사당 1행으로 비정규화 저장.
CREATE TABLE IF NOT EXISTS dart_fundamentals (
    corp_code     VARCHAR(8)   NOT NULL REFERENCES dart_companies(corp_code),
    stock_code    VARCHAR(6)   NOT NULL,
    bsns_year     VARCHAR(4)   NOT NULL,
    fs_div        VARCHAR(4)   NOT NULL,   -- CFS=연결, OFS=별도 (실제 사용된 쪽)
    revenue       BIGINT,                  -- 매출액 (당기)
    revenue_prev  BIGINT,                  -- 매출액 (전기, YoY 계산용)
    net_income    BIGINT,                  -- 당기순이익 (당기)
    equity        BIGINT,                  -- 자본총계 (당기)
    liabilities   BIGINT,                  -- 부채총계 (당기)
    assets        BIGINT,                  -- 자산총계 (당기)
    -- 2026-08-11: QVM(퀄리티+밸류+모멘텀) 복합 팩터용 추가 — 매출원가/매출총이익(IS),
    -- 영업활동현금흐름/유형자산+무형자산의 취득(CF)은 기존과 동일한 fnlttSinglAcntAll
    -- 응답에 이미 포함돼 있어 신규 API 호출 없이 추출 로직만 확장하면 된다.
    cogs                BIGINT,            -- 매출원가 (당기)
    gross_profit        BIGINT,            -- 매출총이익 (당기)
    operating_cash_flow BIGINT,            -- 영업활동현금흐름 (당기)
    capex               BIGINT,            -- 유형자산+무형자산의 취득 합계 (당기, 설비투자 근사)
    fetched_at    TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (corp_code, bsns_year)
);
CREATE INDEX IF NOT EXISTS idx_dart_fundamentals_stock_code
    ON dart_fundamentals (stock_code);
"""

_CREATE_SECTOR_DAILY_STATS = """
CREATE TABLE IF NOT EXISTS sector_daily_stats (
    sector          TEXT        NOT NULL,
    trade_date      DATE        NOT NULL,
    ticker_count    INT         NOT NULL DEFAULT 0,
    avg_return_pct  FLOAT,          -- 당일 평균 수익률 (Close/prev_Close - 1)
    foreign_net_sum BIGINT,         -- 섹터 외국인 순매수 합계
    inst_net_sum    BIGINT,         -- 섹터 기관 순매수 합계
    avg_flow_score  FLOAT,          -- 평균 flow_score (stage_classifications 기준)
    stage1_count    INT,            -- 당일 Stage 1 종목 수
    stage2_count    INT,            -- 당일 Stage 2 종목 수
    stage3_count    INT,            -- 당일 Stage 3 종목 수
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (sector, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_sector_daily_stats_date
    ON sector_daily_stats (trade_date DESC);
"""

# 2026-09-12: 액면병합/분할 등으로 매매거래 정지된 종목 추적용 — 208860.KQ
# (액면병합, 09-02부터 정지) 발견이 계기. paper_exit_checker_job이 정지 감지
# 시점의 krx_listings.listed_shares/par_value를 스냅샷해두고, 재개가 감지돼도
# resolved=TRUE로 사람이 수동 확정하기 전까지는 청산 판정을 계속 스킵한다 —
# 그렇지 않으면 재개 직후 병합비율만큼 튄 가격을 그대로 실제 수익으로 계산해
# hard_stop/trail이 오발동한다(gen1 문서가 경고한 "근거 없는 근사치로 실전
# 성과 통계를 오염시키는" 패턴과 동일한 위험).
_CREATE_PAPER_HALT_WATCH = """
CREATE TABLE IF NOT EXISTS paper_halt_watch (
    ticker               TEXT        PRIMARY KEY,
    detected_date        DATE        NOT NULL,
    listed_shares_before BIGINT,
    par_value_before     TEXT,
    resumed_date         DATE,
    resolved             BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);
"""


# ── RLS 활성화 (Supabase PostgREST 노출 차단) ────────────────────
# 이 백엔드는 asyncpg 직접 연결(postgres/service_role)을 사용하므로 RLS 영향 없음.
# anon/authenticated 롤이 PostgREST를 통해 테이블에 접근하는 것만 차단.
# 주의: 배치 실행 금지. 한 테이블이 없으면 나머지도 모두 실패하므로 개별 실행.
# aftermarket_snap / paper_positions 는 별도 스크립트에서 생성 → 여기선 IF EXISTS 형태로 처리.
_RLS_ALWAYS: list[str] = [
    "news_articles",
    "trade_signals",
    "daily_ohlcv",
    "daily_flow",
    "chart_signals",
    "stage_classifications",
    "watchlist_vol_log",
    "intraday_volumes",
    "krx_listings",
    "ticker_names",
    "trade_log",
    "scheduler_triggers",
    "dart_companies",
    "dart_disclosures",
    "dart_xbrl",
    "dart_segments",
    "dart_fundamentals",
    "sector_daily_stats",
    "paper_halt_watch",
]

# init_db 호출 시점에 아직 없을 수 있는 테이블 — DO 블록으로 안전하게 처리
_RLS_IF_EXISTS: list[str] = [
    "aftermarket_snap",   # krx/kiwoom_aftermarket_sync.py ensure_table()에서 생성
    "paper_positions",    # kiwoom_paper_trader.init_paper_positions()에서 생성
    "apscheduler_jobs",   # run_scheduler.py SQLAlchemyJobStore가 scheduler.start() 시 생성
]


async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE)
        await conn.execute(_CREATE_TRADE_LOG)
        await conn.execute(_CREATE_KRX_TABLE)
        await conn.execute(_CREATE_TICKER_NAMES)
        await conn.execute(_CREATE_SCHEDULER_TRIGGERS)
        await conn.execute(_CREATE_DART_TABLES)
        await conn.execute(_CREATE_SECTOR_DAILY_STATS)
        await conn.execute(_CREATE_PAPER_HALT_WATCH)
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS sector VARCHAR(80) DEFAULT ''"
        )
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS ma_120w FLOAT"
        )
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS high_w FLOAT"
        )
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS volume_w BIGINT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS s1_volume BIGINT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS s1_txamt BIGINT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS foreign_chg_14d_pct FLOAT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS flow_score FLOAT"
        )
        await conn.execute(
            "ALTER TABLE watchlist_vol_log ADD COLUMN IF NOT EXISTS s1_txamt BIGINT"
        )
        # 2026-08-11: QVM 복합 팩터용 — 기존에 채워진 행이 있으므로 CREATE TABLE IF
        # NOT EXISTS만으로는 컬럼이 추가되지 않아 ALTER TABLE로 별도 반영.
        await conn.execute(
            "ALTER TABLE dart_fundamentals ADD COLUMN IF NOT EXISTS cogs BIGINT"
        )
        await conn.execute(
            "ALTER TABLE dart_fundamentals ADD COLUMN IF NOT EXISTS gross_profit BIGINT"
        )
        await conn.execute(
            "ALTER TABLE dart_fundamentals ADD COLUMN IF NOT EXISTS operating_cash_flow BIGINT"
        )
        await conn.execute(
            "ALTER TABLE dart_fundamentals ADD COLUMN IF NOT EXISTS capex BIGINT"
        )
        # RLS: 반드시 존재하는 테이블은 개별 실행 (한 번에 보내면 한 테이블 실패 시 전체 롤백)
        for _tbl in _RLS_ALWAYS:
            await conn.execute(
                f"ALTER TABLE {_tbl} ENABLE ROW LEVEL SECURITY;"
            )
            await conn.execute(f"""
                DO $$ BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_policies
                    WHERE schemaname='public' AND tablename='{_tbl}' AND policyname='backend_all'
                  ) THEN
                    CREATE POLICY backend_all ON {_tbl} FOR ALL TO service_role USING (true) WITH CHECK (true);
                  ELSE
                    ALTER POLICY backend_all ON {_tbl} TO service_role;
                  END IF;
                END $$;
            """)
        # RLS: init_db 시점에 아직 없을 수 있는 테이블 — IF EXISTS 가드
        for _tbl in _RLS_IF_EXISTS:
            await conn.execute(
                f"""
                DO $$ BEGIN
                  IF EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = '{_tbl}'
                  ) THEN
                    ALTER TABLE {_tbl} ENABLE ROW LEVEL SECURITY;
                    IF NOT EXISTS (
                      SELECT 1 FROM pg_policies
                      WHERE schemaname='public' AND tablename='{_tbl}' AND policyname='backend_all'
                    ) THEN
                      CREATE POLICY backend_all ON {_tbl} FOR ALL TO service_role USING (true) WITH CHECK (true);
                    ELSE
                      ALTER POLICY backend_all ON {_tbl} TO service_role;
                    END IF;
                  END IF;
                END $$;
                """
            )
    logger.info("DB 테이블 준비 완료 (news_articles, stage_classifications, krx_listings, …)")
