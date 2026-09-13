"""거래정지 의심 종목 감지 테스트.

2026-09-12: 208860.KQ(다산디엠씨)가 액면병합으로 09-02부터 매매거래 정지된
채 daily_ohlcv엔 종가가 얼어붙어 남아있는 걸 발견 — 방치하면 재개 후
병합비율만큼 가격이 튈 때 hard_stop/trail이 잘못 발동해 청산될 위험이 있다
(gen1 문서가 기록한 "운영 이벤트가 실전 성과 통계를 오염시키는" 패턴과 동일).

KiwoomPaperTrader.is_trading_halted(): ka10001에 정지 전용 필드가 없어
trde_qty·open_pric이 둘 다 '0'인지로 근사 판정(정상 거래 종목은 장마감
후에도 trde_qty가 남아있음 — 005930/000660 대조로 검증).

paper_exit_checker_job(): 정지 의심 종목은 청산 판정 자체를 스킵하고,
텔레그램으로 한 번에 모아 알림.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ═════════════════════════════════════════════════════════════
# KiwoomPaperTrader.is_trading_halted()
# ═════════════════════════════════════════════════════════════

def _make_trader(mock_post=None, quote_client_none=False):
    """KiwoomPaperTrader.__init__을 건너뛰어 인증/네트워크 없이 인스턴스 생성
    (tests/test_kiwoom_execution_check.py의 _make_trader와 동일 패턴)."""
    from data.kiwoom_paper_trader import KiwoomPaperTrader

    trader = object.__new__(KiwoomPaperTrader)
    if quote_client_none:
        trader._quote_client = None
    else:
        client = MagicMock()
        client._post = mock_post
        trader._quote_client = client
    return trader


class TestIsTradingHalted:
    def test_halted_when_qty_and_open_zero(self):
        """208860.KQ 실측 응답 형태 — trde_qty·open_pric 전부 '0' → 정지로 판정."""
        mock_post = MagicMock(return_value=(
            {"trde_qty": "0", "open_pric": "0", "high_pric": "0", "low_pric": "0",
             "cur_prc": "1303", "base_pric": "1303"}, {}
        ))
        trader = _make_trader(mock_post)
        assert trader.is_trading_halted("208860.KQ") is True

    def test_not_halted_when_actively_traded(self):
        """005930 실측 응답 형태 — trde_qty가 실제 값 → 정상 거래로 판정."""
        mock_post = MagicMock(return_value=(
            {"trde_qty": "13939111", "open_pric": "-258000",
             "cur_prc": "-259500", "base_pric": "269000"}, {}
        ))
        trader = _make_trader(mock_post)
        assert trader.is_trading_halted("005930.KS") is False

    def test_no_quote_client_returns_false(self):
        """실 API 시세 클라이언트 미설정 — 판단 불가는 False(정상 취급)."""
        trader = _make_trader(quote_client_none=True)
        assert trader.is_trading_halted("208860.KQ") is False

    def test_lookup_failure_returns_false(self):
        """API 호출 실패도 False(정상 취급) — 청산 로직을 막지 않는다."""
        mock_post = MagicMock(side_effect=RuntimeError("network error"))
        trader = _make_trader(mock_post)
        assert trader.is_trading_halted("208860.KQ") is False


# ═════════════════════════════════════════════════════════════
# record_halt_detected() — 재정지(re-halt) 재무장 쿼리 구조 확인
# ═════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_record_halt_detected_query_rearms_on_resolved():
    """resolved=TRUE인 기존 행을 다시 무장(resolved=FALSE, resumed_date=NULL)
    시키는 조건부 UPDATE가 ON CONFLICT 절에 들어있는지 확인 — 이미
    resolved=TRUE로 확정됐던 티커가 별개 사유로 다시 정지됐을 때, 두 번째
    사건이 재개 후 보호를 못 받고 새는 걸 막는 조건이다(실제 쿼리 동작은
    이 세션에서 실제 Supabase DB로 별도 검증 완료)."""
    from data.kiwoom_paper_trader import record_halt_detected

    conn = AsyncMock()
    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)

    await record_halt_detected(pool, "TEST.KQ", 200, "2500")

    query = conn.execute.call_args.args[0]
    assert "ON CONFLICT (ticker) DO UPDATE" in query
    assert "resolved=FALSE" in query
    assert "resumed_date=NULL" in query
    assert "WHERE paper_halt_watch.resolved = TRUE" in query
    assert conn.execute.call_args.args[1:] == ("TEST.KQ", 200, "2500")


# ═════════════════════════════════════════════════════════════
# paper_exit_checker_job() — 정지 의심 종목 스킵 + 텔레그램 알림
# ═════════════════════════════════════════════════════════════

def _pos(id_, ticker, model, entry=1000.0, qty=10, signal_date=None):
    return {
        "id": id_, "ticker": ticker, "model": model,
        "entry_actual": entry, "entry_theory": entry,
        "hard_stop_pct": 0.10, "tp1_pct": 0.15, "tp1_ratio": 0.50,
        "trail_pct": 0.10, "tp1_date": None, "tp1_price": None,
        "watermark": None, "signal_date": signal_date or (date.today() - timedelta(days=1)),
        "qty": qty,
    }


@pytest.mark.asyncio
async def test_halted_position_skipped_and_not_closed():
    """정지 의심 종목은 가격이 있어도 청산 판정을 타지 않는다 — update_to_closed
    미호출, 정상 종목은 그대로 판정된다. 최초 감지라 record_halt_detected가 호출된다."""
    from jobs.paper_jobs import paper_exit_checker_job

    positions = [
        _pos(1, "208860.KQ", "kosdaq", entry=1225.0),
        _pos(2, "005930.KS", "stage", entry=70000.0),
    ]

    trader = MagicMock()
    trader.get_position_qty.return_value = 10  # _reconcile_stale_positions no-op
    trader.get_current_price.side_effect = lambda tk: {
        "208860.KQ": 1303, "005930.KS": 70000,
    }.get(tk)
    trader.is_trading_halted.side_effect = lambda tk: tk == "208860.KQ"

    mock_closed = AsyncMock()
    mock_post = AsyncMock()
    mock_detected = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_closed),
        patch("jobs.paper_jobs._post_message", mock_post),
        patch("jobs.paper_jobs.get_halt_watch", AsyncMock(return_value=None)),
        patch("jobs.paper_jobs.get_listed_shares", AsyncMock(return_value=(None, None))),
        patch("jobs.paper_jobs.record_halt_detected", mock_detected),
        patch("jobs.paper_jobs.record_halt_resumed", AsyncMock()),
        patch("jobs.paper_jobs.get_unresolved_halts", AsyncMock(return_value=[])),
    ):
        await paper_exit_checker_job(MagicMock(), trader)

    mock_closed.assert_not_called()
    mock_detected.assert_called_once()
    assert mock_detected.call_args.args[1] == "208860.KQ"

    # 정지 알림이 한 건으로 모여서 전송됨
    halt_calls = [c for c in mock_post.call_args_list
                  if "거래정지 의심" in c.args[3]]
    assert len(halt_calls) == 1
    msg = halt_calls[0].args[3]
    assert "208860.KQ" in msg
    assert "005930.KS" not in msg


@pytest.mark.asyncio
async def test_no_halted_positions_sends_no_halt_alert():
    """정지 의심 종목이 없으면 정지 알림 자체를 보내지 않는다."""
    from jobs.paper_jobs import paper_exit_checker_job

    positions = [_pos(1, "005930.KS", "stage", entry=70000.0)]

    trader = MagicMock()
    trader.get_position_qty.return_value = 10
    trader.get_current_price.return_value = 70000
    trader.is_trading_halted.return_value = False

    mock_post = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", AsyncMock()),
        patch("jobs.paper_jobs._post_message", mock_post),
        patch("jobs.paper_jobs.get_halt_watch", AsyncMock(return_value=None)),
        patch("jobs.paper_jobs.get_listed_shares", AsyncMock(return_value=(None, None))),
        patch("jobs.paper_jobs.record_halt_detected", AsyncMock()),
        patch("jobs.paper_jobs.record_halt_resumed", AsyncMock()),
        patch("jobs.paper_jobs.get_unresolved_halts", AsyncMock(return_value=[])),
    ):
        await paper_exit_checker_job(MagicMock(), trader)

    halt_calls = [c for c in mock_post.call_args_list
                  if "거래정지 의심" in c.args[3]]
    assert len(halt_calls) == 0


@pytest.mark.asyncio
async def test_resumed_but_unresolved_still_skipped_with_ratio_hint():
    """정지가 풀려 is_trading_halted=False가 됐어도, paper_halt_watch가
    resolved=False로 남아있으면 여전히 청산 판정을 스킵하고, 재개+병합비율
    추정치를 텔레그램으로 알린다 — 이게 이 기능의 핵심(재개 직후 병합 전
    entry_actual/qty로 잘못 청산되는 걸 막는 것)이다."""
    from jobs.paper_jobs import paper_exit_checker_job

    positions = [_pos(1, "208860.KQ", "kosdaq", entry=1225.0)]

    pool = MagicMock()
    trader = MagicMock()
    trader.get_position_qty.return_value = 10
    trader.get_current_price.return_value = 6500  # 재개 후 병합비율만큼 튄 가격
    trader.is_trading_halted.return_value = False  # 이미 재개됨

    watch_row = {
        "ticker": "208860.KQ", "resumed_date": None, "resolved": False,
        "listed_shares_before": 33_949_973,
    }

    mock_closed = AsyncMock()
    mock_post = AsyncMock()
    mock_resumed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_closed),
        patch("jobs.paper_jobs._post_message", mock_post),
        patch("jobs.paper_jobs.get_halt_watch", AsyncMock(return_value=watch_row)),
        patch("jobs.paper_jobs.get_listed_shares", AsyncMock(return_value=(6_789_994, "2500"))),
        patch("jobs.paper_jobs.record_halt_detected", AsyncMock()),
        patch("jobs.paper_jobs.record_halt_resumed", mock_resumed),
        patch("jobs.paper_jobs.get_unresolved_halts",
              AsyncMock(return_value=[{"ticker": "208860.KQ"}])),
    ):
        await paper_exit_checker_job(pool, trader)

    mock_closed.assert_not_called()
    mock_resumed.assert_called_once_with(pool, "208860.KQ")

    resume_calls = [c for c in mock_post.call_args_list
                    if "재개 감지" in c.args[3]]
    assert len(resume_calls) == 1
    msg = resume_calls[0].args[3]
    assert "208860.KQ" in msg
    assert "5.00배" in msg
    assert "resolve_paper_halt.py" in msg
