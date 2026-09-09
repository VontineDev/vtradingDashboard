"""인프라 잡 — KRX 종목 갱신, 수급 증분 sync."""

import asyncio
import logging
import os
import re
import sys
from typing import Optional

logger = logging.getLogger(__name__)

# daily_flow_sync_job()에는 서로를 모르는 3개의 독립 진입 경로가 있다 —
# APScheduler cron(평일 09:00 UTC=18:00 KST), 대시보드→scheduler_triggers
# DB 폴링(jobs/scheduler_jobs.py::_trigger_watcher_job), 텔레그램 /run_flow·
# /run_all(telegram/bot_handlers.py). 텔레그램 쪽만 자체 락(_flow_lock)이
# 있었고 나머지 둘은 그 존재를 몰라, 2026-08-31 18:00 cron 실행이 아직
# 안 끝난 상태에서 /run_all이 같은 krx_flow_sync.py 서브프로세스를 하나 더
# 띄워 ~9분간 동시 실행되는 사고가 났다(둘 다 같은 KRX 세션/API를 두고
# 경합 — save_daily_flow가 upsert라 데이터 오염까지는 안 갔지만 무의미한
# 중복 작업 + 세션/rate-limit 경합 위험). 호출부마다 락을 챙기게 하는 대신
# 실제 자원을 쓰는 이 함수 자체에 락을 둬서, 세 경로 전부가 자동으로
# 보호받게 한다(단일 진실 공급원). telegram/telegram_bot.py의 _flow_lock은
# 이 락을 그대로 re-export한 것 — 같은 객체이므로 텔레그램 쪽에서 또
# async with로 감싸면 자기 자신을 기다리며 데드락 나니 감싸면 안 됨
# (텔레그램은 .locked()로 미리 훑어보고 안내 메시지만 내보낸다).
flow_sync_lock: asyncio.Lock = asyncio.Lock()

_SUBPROCESS_LOG_LEVEL_RE = re.compile(r"\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]")
_SUBPROCESS_LOG_FNS = {
    "DEBUG": logger.debug,
    "INFO": logger.info,
    "WARNING": logger.warning,
    "ERROR": logger.error,
    "CRITICAL": logger.critical,
}


def _relog_subprocess_line(line: str) -> None:
    """서브프로세스 출력 라인을 원래 로그 레벨 그대로 재기록.

    krx_flow_sync.py는 이미 logging level=INFO로 필터링된 라인만 stdout에
    내보내므로, 여기서 전부 DEBUG로 깔아버리면(레벨 무관) WARNING/ERROR가
    스케줄러 로그(INFO)에서 안 보여 세션 만료 같은 실패 신호가 묻힌다.
    """
    m = _SUBPROCESS_LOG_LEVEL_RE.search(line)
    log_fn = _SUBPROCESS_LOG_FNS.get(m.group(1), logger.info) if m else logger.info
    log_fn("[flow-sync] %s", line)


async def daily_krx_refresh_job(db_pool) -> None:
    from data.krx_sync import sync_krx_listings
    from core.ticker_cache import ticker_cache
    try:
        n = await sync_krx_listings(db_pool)
        logger.info("[krx_sync] 일일 갱신 완료: %d행", n)
    except Exception as e:
        logger.error("[krx_sync] 일일 갱신 실패: %s — 기존 DB 데이터로 캐시 재로드", e)
    finally:
        try:
            await ticker_cache.load(db_pool)
        except Exception as _cache_e:
            logger.warning("[ticker_cache] 캐시 재로드 실패: %s", _cache_e)


async def daily_dart_disclosure_job(db_pool) -> None:
    """평일 09:00 KST — 전일 Top 20 기업 공시 이벤트 수집.

    스케줄러 재시작 등으로 수집이 끊겼을 경우, 마지막 수집일 다음날부터
    오늘까지 자동 백필한다 (최대 90일).
    """
    from datetime import date, timedelta
    from data.dart_sync import get_top20_corp_codes, sync_disclosures

    try:
        corp_codes = await get_top20_corp_codes(db_pool)
        if not corp_codes:
            logger.warning("[dart] Top 20 corp_code 없음 — seed-companies 선행 필요")
            return

        # 마지막 수집일 확인 (rcept_dt 기준)
        async with db_pool.acquire() as conn:
            last_dt = await conn.fetchval(
                "SELECT MAX(rcept_dt) FROM dart_disclosures"
            )

        today = date.today()
        yesterday = today - timedelta(days=1)

        if last_dt and (yesterday - last_dt).days > 1:
            # 갭 감지: 마지막 수집일 다음날부터 어제까지 백필 (최대 90일)
            gap_start = last_dt + timedelta(days=1)
            if (yesterday - gap_start).days > 90:
                gap_start = yesterday - timedelta(days=89)
            bgn_de = gap_start.strftime("%Y%m%d")
            end_de = yesterday.strftime("%Y%m%d")
            logger.info("[dart] 공시 갭 감지 (%s ~ %s) — 백필 시작", bgn_de, end_de)
            n = await sync_disclosures(db_pool, corp_codes, bgn_de, end_de)
            logger.info("[dart] 공시 백필 완료: %d건 (%s ~ %s)", n, bgn_de, end_de)
        else:
            # 정상: 전일 하루치만 수집
            n = await sync_disclosures(db_pool, corp_codes)
            logger.info("[dart] 일별 공시 수집 완료: %d건", n)

    except Exception as e:
        logger.error("[dart] 일별 공시 수집 실패: %s", e)


async def annual_dart_segments_job(db_pool) -> None:
    """매년 5월 1일 03:00 KST — Top 20 사업보고서 II-2/II-4 Ollama 파싱."""
    from data.dart_sync import get_top20_corp_codes, sync_segments
    try:
        corp_codes = await get_top20_corp_codes(db_pool)
        if not corp_codes:
            logger.warning("[dart] Top 20 corp_code 없음 — seed-companies 선행 필요")
            return
        from datetime import date
        bsns_year = str(date.today().year - 1)
        n = await sync_segments(db_pool, corp_codes, bsns_year)
        logger.info("[dart] 연간 세그먼트 파싱 완료: %d건 (%s)", n, bsns_year)
    except Exception as e:
        logger.error("[dart] 연간 세그먼트 파싱 실패: %s", e)


async def annual_dart_extractor_job(db_pool) -> None:
    """매년 5월 1일 03:00 KST — Top20 사업보고서 로컬 XML Ollama 추출 → dart_extractions."""
    from data.dart_extractor import extract_all
    try:
        n = await extract_all(db_pool)
        logger.info("[dart-extractor] 완료: %d건", n)
    except Exception as e:
        logger.error("[dart-extractor] 실패: %s", e)


async def monthly_dart_xbrl_job(db_pool) -> None:
    """매월 1일 02:00 KST — Top 20 기업 전년도 XBRL 재무수치 갱신."""
    from data.dart_sync import get_top20_corp_codes, sync_xbrl
    try:
        corp_codes = await get_top20_corp_codes(db_pool)
        if not corp_codes:
            logger.warning("[dart] Top 20 corp_code 없음 — seed-companies 선행 필요")
            return
        n = await sync_xbrl(db_pool, corp_codes)
        logger.info("[dart] 월별 XBRL 갱신 완료: %d건", n)
    except Exception as e:
        logger.error("[dart] 월별 XBRL 갱신 실패: %s", e)


async def daily_market_snap_job() -> None:
    """ka10032 당일 누적 스냅샷 → daily_market_snap upsert.

    1차: 평일 16:10 KST — NXT 단일가 종료 후 중간 스냅샷 (히트맵/TOP 즉시 반영용)
    2차: 평일 20:10 KST — NXT 애프터마켓 종료 후 완전한 KRX+NXT 최종값으로 덮어씀

    stex_tp=3 (KRX+NXT 합산) 전종목(거래대금>0, cont-yn 페이지네이션). (ticker, trade_date)
    upsert라 중복 없음.
    """
    from datetime import date as _date
    from core.db import get_dsn as _get_dsn
    from data.kiwoom_aftermarket_sync import (
        _build_client, run_daily_snap,
    )
    logger.info("[daily-snap] 시작")
    try:
        dsn = _get_dsn()
        client = _build_client()
        saved = run_daily_snap(dsn, client, _date.today())
        if saved == 0:
            logger.warning("[daily-snap] 저장된 데이터 없음")
    except Exception as e:
        logger.error("[daily-snap] 실패: %s", e)


async def daily_aftermarket_sync_job() -> None:
    """평일 16:05 KST — NXT 시간외 단일가 + ka10032 합산 거래대금 수집.

    kiwoom_aftermarket_sync.py --incremental 을 서브프로세스로 실행.
    trade_date = 전일 영업일로 저장 (--incremental 기본 동작).
    """
    logger.info("[aftermarket-sync] kiwoom_aftermarket_sync --incremental 시작")
    try:
        _root   = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
        _script = os.path.join(_root, "data", "kiwoom_aftermarket_sync.py")
        _env    = {**os.environ, "PYTHONPATH": _root}
        proc = await asyncio.create_subprocess_exec(
            sys.executable, _script, "--incremental",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=_env,
        )
        out, _ = await proc.communicate()
        if proc.returncode == 0:
            logger.info("[aftermarket-sync] 완료 (exit=0)")
        else:
            logger.warning("[aftermarket-sync] 비정상 종료 (exit=%d)", proc.returncode)
        if out:
            for line in out.decode("utf-8", errors="replace").splitlines():
                if line.strip():
                    logger.debug("[aftermarket-sync] %s", line)
    except Exception as e:
        logger.warning("[aftermarket-sync] 실행 실패: %s", e)


# cron(평일 09:05 KST)·대시보드 트리거(scheduler_triggers 폴링)·텔레그램
# (/run_youtube, /run_all) 3개의 독립 경로가 youtube_narrative_sync_job()을
# 부를 수 있다(flow_sync_lock과 동일 구조·동일 사고 위험, 2026-08-31 발견).
# 실제 Tor 회로/LLM 서버를 공유하는 이 함수만 락으로 보호 — 뒤이어 도는
# youtube_attention_score_job()은 로컬 DB 집계라 자원 경합 위험이 낮아
# 대상에서 뺐다.
youtube_sync_lock: asyncio.Lock = asyncio.Lock()


async def youtube_narrative_sync_job() -> None:
    """평일 09:05 KST — 전일 삼프로TV 업로드 수집 + LLM 추출 → youtube_mention_raw."""
    if youtube_sync_lock.locked():
        logger.warning("[yt-sync] 이미 다른 실행이 진행 중 — 이번 트리거는 건너뜀 (중복 실행 방지)")
        return

    async with youtube_sync_lock:
        import os
        from datetime import date as _date, timedelta as _td
        from core.db import get_dsn as _get_dsn
        from data.youtube_narrative_sync import run_sync, ensure_tables
        logger.info("[yt-sync] 운영 수집 시작")
        try:
            dsn        = _get_dsn()
            api_key = os.environ.get("YOUTUBE_API_KEY", "")
            if not api_key:
                logger.warning("[yt-sync] YOUTUBE_API_KEY 미설정 — 건너뜀")
                return
            ensure_tables(dsn)
            yesterday = _date.today() - _td(days=1)
            n = await asyncio.to_thread(run_sync, dsn, api_key, yesterday, yesterday)
            logger.info("[yt-sync] 완료: %d건", n)
        except Exception as e:
            logger.error("[yt-sync] 실패: %s", e)


async def youtube_attention_score_job() -> None:
    """평일 09:10 KST — rolling 5영업일 attention_score 집계."""
    from core.db import get_dsn as _get_dsn
    from data.youtube_narrative_sync import compute_attention_scores
    logger.info("[yt-attn] 집계 시작")
    try:
        n = await asyncio.to_thread(compute_attention_scores, _get_dsn())
        logger.info("[yt-attn] 완료: %d종목", n)
    except Exception as e:
        logger.error("[yt-attn] 실패: %s", e)


async def youtube_forward_return_job() -> None:
    """평일 15:40 KST — 당일 종가로 forward return 백필."""
    from core.db import get_dsn as _get_dsn
    from data.youtube_narrative_sync import fill_forward_returns
    logger.info("[yt-ret] forward return 채우기 시작")
    try:
        n = await asyncio.to_thread(fill_forward_returns, _get_dsn())
        logger.info("[yt-ret] 완료: %d건", n)
    except Exception as e:
        logger.error("[yt-ret] 실패: %s", e)


async def dart_screened_sync_job(
    db_pool,
    days:  int  = 30,
    limit: int  = 30,
    force: bool = False,
) -> None:
    """스크리닝 후 자동 DART 분석 — 최근 days일 Stage/스크리너 종목 대상.

    screener_job / stage_job 완료 후 호출하거나 독립 스케줄로 실행.
    Ollama 미응답 시 조용히 종료 (스케줄러 크래시 방지).
    """
    try:
        from scripts.dart_screened_sync import dart_screened_sync_job as _impl
        stats = await _impl(db_pool, days=days, limit=limit, force=force)
        logger.info(
            "[dart-screened] 잡 완료 — 추출:%d 스킵:%d 실패:%d",
            stats["extracted"], stats["skipped"], stats["failed"],
        )
    except Exception as e:
        logger.error("[dart-screened] 잡 실패: %s", e)


async def daily_ohlcv_warm_job() -> None:
    """평일 18:30 KST — 전일 전 종목 OHLCV를 daily_ohlcv에 채우기.

    KRX OpenAPI(get_daily_ohlcv_all) 사용. KRX_OPENAPI_KEY 미설정 시 경고 후 skip.
    daily_flow_sync(18:00) 완료 후 30분 뒤 실행.
    """
    from core.db import get_dsn as _get_dsn
    from jobs.ohlcv_warm import daily_ohlcv_warm_job as _impl
    try:
        dsn = _get_dsn()
        if not dsn:
            logger.warning("[ohlcv-warm] DSN 미설정 — 스킵")
            return
        import asyncio
        loop = asyncio.get_running_loop()
        n = await loop.run_in_executor(None, _impl, dsn)
        logger.info("[ohlcv-warm] 일배치 완료: %d종목", n)
    except Exception as e:
        logger.error("[ohlcv-warm] 일배치 실패: %s", e)


async def daily_flow_sync_job() -> None:
    """평일 18:00 KST — krx-direct(data.krx.co.kr)로 전일 수급 증분 적재.

    kiwoom 백엔드(ka10045)는 기관/외국인만 제공하고 personal_net을 주지 않아
    classify_stage_v15의 Stage2 "개인 출회" 게이트가 무력화되는 문제가 있었음
    (TODOS.md 기록 참고) — krx-direct로 되돌려 개인 순매수까지 정확히 채움.
    KRX_SESSION 쿠키가 만료되면 이 잡이 실패하므로 .env 갱신이 필요할 수 있다.

    cron·대시보드 트리거·텔레그램 3개 경로가 동시에 호출할 수 있어
    flow_sync_lock으로 보호한다 — 이미 실행 중이면 대기하지 않고 즉시
    건너뛴다(cron 경로가 텔레그램 실행을 몇 시간씩 기다리게 두는 것보다,
    다음 스케줄/재시도에 맡기는 편이 낫다).
    """
    if flow_sync_lock.locked():
        logger.warning("[flow-sync] 이미 다른 실행이 진행 중 — 이번 트리거는 건너뜀 "
                        "(중복 실행 방지, 2026-08-31 cron/텔레그램 동시실행 사고 계기)")
        return

    async with flow_sync_lock:
        logger.info("[flow-sync] krx_flow_sync --incremental --backend krx-direct 시작")
        try:
            _root   = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
            _script = os.path.join(_root, "data", "krx_flow_sync.py")
            _env    = {**os.environ, "PYTHONPATH": _root}
            proc = await asyncio.create_subprocess_exec(
                sys.executable, _script, "--incremental", "--backend", "krx-direct",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=_env,
            )
            out, _ = await proc.communicate()
            out_text = out.decode("utf-8", errors="replace") if out else ""
            if proc.returncode == 0:
                logger.info("[flow-sync] 완료 (exit=0)")
            else:
                # krx_flow_sync.py가 KrxTerminalAuthError(CD010 비밀번호 변경
                # 필요, CD007 로그인 시도 초과 잠금 등 — 원인 코드는 실행마다
                # 다를 수 있음)로 즉시 중단한 경우, 이건 세션 만료가 아니라
                # 계정 자체가 막힌 상태라 KRX_SESSION을 아무리 갱신해도 안
                # 풀린다 (2026-09-07/09-09 실사고: 이 구분 없이 "세션 만료
                # 의심"으로만 안내해 사람이 엉뚱한 조치를 하며 낭비할 뻔함).
                # "계정 자체가 막힌 상태"는 krx_flow_sync.py main()의 로그와
                # 맞춰둔 고정 마커 — 특정 에러 코드에 매지 말 것(다음에 또
                # 다른 코드가 나올 수 있음). 실제 원인은 로그에서 [코드] 뒤
                # 메시지를 뽑아 알림에 그대로 실어 보낸다.
                terminal_match = re.search(
                    r"KRX 로그인 실패 \[(\S+)\] — 계정 자체가 막힌 상태.*?: (.+)", out_text
                )
                if terminal_match:
                    err_code, err_msg = terminal_match.group(1), terminal_match.group(2).strip()
                    logger.warning(
                        "[flow-sync] 비정상 종료 (exit=%d) — KRX 계정 자체가 막힌 상태 "
                        "[%s]: %s (KRX_SESSION 만료 아님)", proc.returncode, err_code, err_msg,
                    )
                    await _alert_flow_sync_failure(
                        f"daily_flow_sync_job 비정상 종료 (exit={proc.returncode})",
                        account_blocked_detail=f"[{err_code}] {err_msg}",
                    )
                else:
                    logger.warning("[flow-sync] 비정상 종료 (exit=%d) — KRX_SESSION 만료 의심", proc.returncode)
                    await _alert_flow_sync_failure(f"daily_flow_sync_job 비정상 종료 (exit={proc.returncode})")
            if out_text:
                for line in out_text.splitlines():
                    if line.strip():
                        _relog_subprocess_line(line)
        except Exception as e:
            logger.warning("[flow-sync] 실행 실패: %s", e)
            await _alert_flow_sync_failure(f"daily_flow_sync_job 실행 실패: {e}")


async def _alert_flow_sync_failure(reason: str, account_blocked_detail: Optional[str] = None) -> None:
    """daily_flow_sync_job 실패를 텔레그램으로 알림 (Tor Browser 꺼짐 등
    조용히 반복 실패하는 것을 막기 위함). 알림 자체가 실패해도 잡을 죽이지
    않는다 — best-effort.

    account_blocked_detail이 주어지면 원인이 KRX 계정 자체가 막힌 상태(예:
    CD010 비밀번호 변경 필요, CD007 로그인 시도 초과 잠금 — 코드는 매번 다를
    수 있어 여기 하드코딩하지 않고 실제 KRX 응답 문구를 그대로 받는다)로
    확정된 경우 — Tor/KRX_SESSION을 확인해보라는 일반 안내 대신 실제 원인과
    정확한 조치를 알려준다(2026-09-07/09-09 사고: 잘못된 안내로 사람이 세션
    쿠키만 갱신 시도하며 낭비할 뻔함).
    """
    try:
        from telegram.telegram_notify import send_admin_alert
        if account_blocked_detail:
            hint = (
                f"KRX 계정 자체가 막힌 상태입니다 {account_blocked_detail} — "
                "data.krx.co.kr에 직접 로그인해서 확인/조치하세요 (KRX_SESSION 문제 아님)."
            )
        else:
            hint = "Tor Browser가 켜져있는지, KRX_SESSION이 유효한지 확인하세요."
        await send_admin_alert(f"{reason}\n{hint}")
    except Exception as e:
        logger.debug("[flow-sync] 실패 알림 전송 실패: %s", e)
