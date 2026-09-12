"""모의투자 잡 — Exit Checker, EOD 샘플러, T+1 진입."""

import asyncio
import logging
import random
from collections import defaultdict
from datetime import date

import httpx

from analysis.backtest.config import (
    OPTIMAL_EXIT_PARAMS          as _KOSPI_P,
    OPTIMAL_EXIT_PARAMS_KOSDAQ   as _KOSDAQ_P,
    OPTIMAL_EXIT_PARAMS_CROSS    as _CROSS_P,
    OPTIMAL_EXIT_PARAMS_ICHIMOKU as _ICHI_P,
)
from core.db import load_chart_signals_latest
from data.kiwoom_paper_trader import (
    ACTIVE_MODELS,
    MODEL_CONFIG,
    get_open_positions,
    update_to_closed,
    get_pending_positions,
    mark_stale_pending_as_held,
    update_to_open,
    _qty_from_price,
    insert_pending,
    get_open_slot_count,
    get_open_or_pending_tickers,
    compute_slot_krw,
    deployable_capital,
    get_listed_shares,
    get_halt_watch,
    record_halt_detected,
    record_halt_resumed,
    get_unresolved_halts,
)
from telegram.telegram_notify import _get_token, _get_chat_id, _post_message

logger = logging.getLogger(__name__)


async def _reconcile_multi_model_ticker(db_pool, paper_trader, _loop, ticker: str, positions: list) -> int:
    """같은 티커를 2개 이상 모델이 동시 보유할 때의 근사 재조정 (2026-08-30, 2026-09-02 부분체결 top-up 확장).

    중복 매매를 모델 독립성을 위해 허용하기로 한 이상, 브로커 계좌가
    모델별로 쪼개지지 않는다는 제약은 그대로 남는다 — 그래서 완전한 정확도
    대신 다음 근사를 쓴다:

    - 목표수량(qty_ordered)에 이미 도달한(qty>=qty_ordered) 포지션은 과거에
      이미 옳게 귀속됐다고 신뢰하고 건드리지 않는다. qty_ordered 자체가
      없는(이 컬럼 도입 이전) 구 데이터도 qty>0이면 같은 취급(목표를 모르니
      현재 값을 그대로 신뢰).
    - qty_ordered가 없는데 qty=0인 행은 "남는 실보유"를 얼마나 줘야 할지 판단
      근거가 없으므로 그룹 전체를 스킵한다(수동 확인 필요).
    - 나머지(목표에 못 미친 행 — 미확인 qty=0이든 부분체결 qty>0이든)는,
      브로커 실보유 중 신뢰 대상(위 두 항목) 및 이 행들이 이미 기록한 만큼을
      뺀 "아직 아무 행에도 귀속 안 된 수량"을 주문 접수 순서(FIFO, created_at
      오름차순 — 먼저 낸 주문이 먼저 채워졌을 것이라는 가정)로 각자의 남은
      목표(qty_ordered - 현재 qty)까지 채운다.
      2026-09-02 발견(036800.KQ, id=178/179): 예전엔 qty>0이면(부분체결
      포함) 무조건 "이미 확정"으로 취급해 다시는 채워주지 않았다 — 단일
      티커 케이스(이 파일 위쪽 _reconcile_stale_positions 본체)는 qty>0이어도
      브로커 실보유가 더 크면 top-up하는데, confirm_fill()의 짧은 폴링창
      때문에 부분체결로 기록된 뒤 나머지가 나중에 체결되는 동일한 근본
      원인이 멀티모델 쪽만 빠져 있어 목표의 19~29%에서 이틀째 영구 고착됐던
      버그.
    - 브로커는 "이 주문이 몇 주 채웠는지"를 모델별로 알려주지 않으므로 이건
      근사치다 — 남는 수량이 목표에 못 미치면 그만큼만 채우고 나머지 모델은
      다음 실행에서 재시도, 신뢰 대상 + 이미 기록된 부분체결 합계가 실보유
      보다 이미 많으면(모델을 특정할 수 없는 매도 발생) 여전히 손대지 않고
      수동 확인으로 넘긴다.
    """
    if any((p["qty"] or 0) < 0 for p in positions):
        logger.info("[paper-exit] %s 같은 티커 %d개 모델 동시보유 — 음수 qty 포함, "
                    "자동 재조정 스킵(수동 확인 필요)", ticker, len(positions))
        return 0

    _ambiguous = [p for p in positions
                  if (p["qty"] or 0) == 0 and p.get("qty_ordered") is None]
    if _ambiguous:
        logger.info("[paper-exit] %s 같은 티커 %d개 모델 동시보유 — qty_ordered 없는 "
                    "구버전 미확인 행 포함, 자동 재조정 스킵(수동 확인 필요)", ticker, len(positions))
        return 0

    _needs_fill = [p for p in positions
                   if p.get("qty_ordered") is not None and (p["qty"] or 0) < p["qty_ordered"]]
    _needs_fill_ids = {p["id"] for p in _needs_fill}
    _settled = [p for p in positions if p["id"] not in _needs_fill_ids]

    if not _needs_fill:
        logger.info("[paper-exit] %s 같은 티커 %d개 모델 동시보유(전부 목표수량 도달) — "
                    "자동 재조정 스킵(수동 확인 필요)", ticker, len(positions))
        return 0

    _actual_total = await _loop.run_in_executor(None, paper_trader.get_position_qty, ticker)
    _claimed_sum = sum(p["qty"] or 0 for p in _settled) + sum(p["qty"] or 0 for p in _needs_fill)

    if _claimed_sum > _actual_total:
        logger.info(
            "[paper-exit] %s 같은 티커 %d개 모델 동시보유 — 이미 기록된 합계(확정+부분체결, %d)가 "
            "실보유(%d)보다 많음(모델 특정 불가한 매도 발생) — 자동 재조정 스킵(수동 확인 필요)",
            ticker, len(positions), _claimed_sum, _actual_total,
        )
        return 0

    _remaining = _actual_total - _claimed_sum
    if _remaining <= 0:
        logger.info("[paper-exit] %s 같은 티커 %d개 모델 동시보유 — 아직 남는 실보유 없음, "
                    "미확인/부분체결 %d건 다음 실행 재시도", ticker, len(positions), len(_needs_fill))
        return 0

    _avg_price = await _loop.run_in_executor(None, paper_trader.get_position_avg_price, ticker)
    if not _avg_price:
        logger.warning("[paper-exit] %s FIFO 귀속용 평균단가 조회 실패 — 다음 실행에서 재시도", ticker)
        return 0

    _n_fixed = 0
    for _pos in sorted(_needs_fill, key=lambda p: p["created_at"]):
        _current = _pos["qty"] or 0
        _target_remaining = _pos["qty_ordered"] - _current
        if _remaining <= 0 or _target_remaining <= 0:
            logger.info("[paper-exit] %s FIFO 귀속: %s(id=%d) 여전히 미확인(순번 대기) — 스킵",
                        ticker, _pos["model"], _pos["id"])
            continue

        _grant = min(_target_remaining, _remaining)
        _remaining -= _grant
        _new_qty = _current + _grant
        try:
            async with db_pool.acquire() as _conn:
                await _conn.execute(
                    "UPDATE paper_positions SET qty=$1, entry_actual=$2 WHERE id=$3",
                    _new_qty, float(_avg_price), _pos["id"],
                )
            _note = "" if _new_qty == _pos["qty_ordered"] else f"(목표 {_pos['qty_ordered']}주 중 {_new_qty}주만 근사귀속 — 나머지는 다음 실행 재시도)"
            logger.info(
                "[paper-exit] %s FIFO 귀속(동시보유 %d개 모델): %s(id=%d) qty %d→%d, entry_actual=%s %s",
                ticker, len(positions), _pos["model"], _pos["id"], _current, _new_qty, _avg_price, _note,
            )
            _n_fixed += 1
        except Exception as _e:
            logger.warning("[paper-exit] %s FIFO 귀속 DB 업데이트 실패(%s, id=%d): %s",
                            ticker, _pos["model"], _pos["id"], _e)
            _remaining += _grant  # 실패했으니 다음 후보에게라도 배분 기회를 되돌려준다

    return _n_fixed


async def _reconcile_stale_positions(db_pool, paper_trader) -> int:
    """직전 실행에서 "매도 체결 미확인"으로 남은 open 포지션을 브로커 실보유와
    대조해 자동 정정.

    2026-08-11 발견: confirm_fill()의 폴링 창(기본 3회×1.5초 ≈ 수 초)이 이
    모의투자 서버의 실제 체결 지연(수 분~10분+, 같은 날 반복 관측)보다
    훨씬 짧아 "같은 실행 안에서 확인"이 구조적으로 거의 불가능하다. 그
    결과 실제로는 (부분)체결된 매도가 매번 "미확인" 상태로 open + 부풀려진
    qty로 남고, 다음 실행에서 그 부풀려진 수량으로 재매도를 시도해
    "매도가능수량 부족"(800033) 에러가 반복 재발한다 — 지난 며칠간 A/B/C
    그룹 유령 보유 정리로 계속 손댄 것과 동일한 근본 원인.

    exit 조건 판정 전에 매번 이 재조정을 먼저 돌려 직전 실행의 미확인분을
    브로커 실보유 기준으로 self-heal한다 — confirm_fill() 자체를 더 오래
    기다리게 만드는 대신(포지션 수만큼 곱해져 잡 실행시간이 비현실적으로
    길어짐), "확인은 다음 실행에서"로 설계를 바꾼 것.

    같은 티커를 여러 모델이 동시 보유하면 브로커 잔고가 모델별로 분리되지
    않는다. 2026-08-30부터 이 경우도 무조건 스킵하지 않고, 이미 체결
    확정된(qty>0) 포지션은 그대로 신뢰하면서 아직 미확인인(qty=0) 포지션들
    사이에서만 "남는 실보유"를 주문 접수 순서(FIFO, created_at)로 근사
    배분한다(_reconcile_multi_model_ticker 참고) — 브로커가 어느 주문 몫인지
    알려주지 않으므로 근사치이며, 이미 줄어든 몫이 있는 경우(체결확정분
    합계가 실보유보다 많음) 등 정말 애매한 경우는 여전히 스킵하고 로그만
    남긴다(수동 확인 필요, 2026-08-11 investigate 세션에서 A/B/C 그룹 정리
    때와 동일한 제약).
    """
    _loop = asyncio.get_running_loop()
    _open_positions = await get_open_positions(db_pool)
    if not _open_positions:
        return 0

    _by_ticker: dict[str, list] = defaultdict(list)
    for _p in _open_positions:
        _by_ticker[_p["ticker"]].append(_p)

    _n_fixed = 0
    for _ticker, _positions in _by_ticker.items():
        if len(_positions) > 1:
            _n_fixed += await _reconcile_multi_model_ticker(
                db_pool, paper_trader, _loop, _ticker, _positions
            )
            continue

        _pos = _positions[0]
        _recorded_qty = _pos["qty"] or 0
        if _recorded_qty < 0:
            continue

        _actual_qty = await _loop.run_in_executor(None, paper_trader.get_position_qty, _ticker)

        if _recorded_qty == 0:
            # 2026-08-13 도입: paper_open_entry_job이 매수 체결 미확인 시 즉시
            # closed 대신 open(qty=0)으로 보류해두는 대상 — 여기서 브로커 실보유
            # 기준으로 최종 판정한다. actual>0이면 아래 "매수 잔량 추가체결"
            # 분기가 그대로 처리하므로(actual>recorded=0), 여기서는 정말로
            # 여전히 0인(최소 하루 경과 후 재확인한) 진짜 미체결 확정만 처리한다.
            if _actual_qty == 0:
                try:
                    _entry_ref = _pos["entry_actual"] or _pos["entry_theory"] or 0.0
                    await update_to_closed(db_pool, _pos["id"], float(_entry_ref),
                                            "buy_never_filled", "", None)
                    logger.info("[paper-exit] %s 매수 미체결 최종 확정(재조정 재확인) → closed 처리",
                                _ticker)
                    _n_fixed += 1
                except Exception as _e:
                    logger.warning("[paper-exit] %s 미체결 확정 DB 업데이트 실패: %s", _ticker, _e)
                continue
        elif _actual_qty == _recorded_qty:
            continue

        if _actual_qty > _recorded_qty:
            # 2026-08-13 발견: 매수 주문이 confirm_fill()의 짧은 폴링 창 안에서는
            # 부분체결로만 확인되고, DB엔 그 수량으로 확정 기록된 뒤 주문이
            # 재확인 없이 방치된다. 남은 잔량이 이후 마저 체결되면 브로커
            # 실보유가 DB보다 커지는데, 이 케이스도 매도 쪽과 동일한 근본
            # 원인(체결 지연 vs 짧은 폴링)이므로 브로커 평균단가(pur_pric)
            # 기준으로 qty/entry_actual을 self-heal한다.
            _avg_price = await _loop.run_in_executor(
                None, paper_trader.get_position_avg_price, _ticker
            )
            if not _avg_price:
                logger.warning(
                    "[paper-exit] %s 브로커 보유(%d)가 DB(%d)보다 많음 — 평균단가 조회 실패, 스킵",
                    _ticker, _actual_qty, _recorded_qty,
                )
                continue
            try:
                async with db_pool.acquire() as _conn:
                    await _conn.execute(
                        "UPDATE paper_positions SET qty=$1, entry_actual=$2 WHERE id=$3",
                        _actual_qty, float(_avg_price), _pos["id"],
                    )
                logger.info(
                    "[paper-exit] %s 매수 잔량 추가체결 재조정 → qty %d→%d, entry_actual=%s (open 유지)",
                    _ticker, _recorded_qty, _actual_qty, _avg_price,
                )
                _n_fixed += 1
            except Exception as _e:
                logger.warning("[paper-exit] %s 매수 잔량 재조정 DB 업데이트 실패: %s", _ticker, _e)
            continue

        # _actual_qty < _recorded_qty: 직전 실행에서 미확인 처리된 매도가 실제로는
        # (부분)체결됐던 것 — 브로커 실보유 기준으로 정정.
        if _actual_qty == 0:
            _price = await _loop.run_in_executor(None, paper_trader.get_current_price, _ticker)
            if not _price:
                logger.warning("[paper-exit] %s 재조정용 현재가 조회 실패 — 다음 실행에서 재시도", _ticker)
                continue
            _entry = _pos["entry_actual"] or _pos["entry_theory"]
            _blended = (float(_price) - _entry) / _entry if _entry else None
            try:
                await update_to_closed(db_pool, _pos["id"], float(_price), "reconciled_prev_fill", "", _blended)
                logger.info("[paper-exit] %s 직전 미확인 매도 재조정 → 전량체결 확인, closed 처리(근사 청산가=%s)",
                            _ticker, _price)
                _n_fixed += 1
            except Exception as _e:
                logger.warning("[paper-exit] %s 재조정 DB 업데이트 실패: %s", _ticker, _e)
        else:
            try:
                async with db_pool.acquire() as _conn:
                    await _conn.execute(
                        "UPDATE paper_positions SET qty=$1 WHERE id=$2", _actual_qty, _pos["id"],
                    )
                logger.info("[paper-exit] %s 직전 미확인 매도 재조정 → qty %d→%d 정정 (open 유지)",
                            _ticker, _recorded_qty, _actual_qty)
                _n_fixed += 1
            except Exception as _e:
                logger.warning("[paper-exit] %s 재조정 qty 업데이트 실패: %s", _ticker, _e)

    return _n_fixed


async def paper_exit_checker_job(db_pool, paper_trader) -> None:
    """정규장 마감 직전(15:20 KST) — 오픈 포지션 전체에 대해 exit 조건 판정 → 시장가 매도주문."""
    today = date.today()
    _loop = asyncio.get_running_loop()

    _n_reconciled = await _reconcile_stale_positions(db_pool, paper_trader)
    if _n_reconciled:
        logger.info("[paper-exit] 직전 실행 미확인분 재조정: %d건", _n_reconciled)

    _open_positions = await get_open_positions(db_pool)
    if not _open_positions:
        logger.info("[paper-exit] 오픈 포지션 없음")
        return

    logger.info("[paper-exit] 오픈 포지션 %d건 exit 체크 시작", len(_open_positions))

    # 현재가 조회 — Kiwoom 실 API ka10001 (정규장 중 실시간 가격; 모의투자 서버는 시장데이터 미지원)
    # 같은 호출 김에 거래정지 여부도 확인(is_trading_halted, 2026-09-12 추가) —
    # 액면병합 등으로 정지된 종목은 가격이 얼어붙은 채로 남아 그대로 청산 판정에
    # 넣으면(특히 재개 후 병합비율만큼 가격이 튈 때) 잘못된 hard_stop/trail 발동으로
    # 이어질 수 있다(208860.KQ 사례). 정지 자체는 일시적이라 is_trading_halted가
    # 재개 즉시 False로 바뀌는데, 그 시점에 바로 판정을 재개하면 여전히 병합 전
    # entry_actual/qty 그대로라 위험이 그대로 남는다 — 그래서 정지 감지 시
    # paper_halt_watch에 상장주식수 스냅샷과 함께 기록해두고, 재개 이후에도
    # 사람이 resolve(비율 확정)하기 전까지는 계속 스킵한다.
    _all_tickers = list({p["ticker"] for p in _open_positions})
    _prices: dict[str, float] = {}
    _live_halted: set[str] = set()
    for _tk in _all_tickers:
        await asyncio.sleep(0.5)
        _px = await _loop.run_in_executor(None, paper_trader.get_current_price, _tk)
        if _px:
            _prices[_tk] = float(_px)
        await asyncio.sleep(0.3)
        if await _loop.run_in_executor(None, paper_trader.is_trading_halted, _tk):
            _live_halted.add(_tk)

    _resume_alerts: list[str] = []
    for _tk in _all_tickers:
        _watch = await get_halt_watch(db_pool, _tk)
        if _tk in _live_halted:
            if not _watch:
                _shares, _par = await get_listed_shares(db_pool, _tk)
                await record_halt_detected(db_pool, _tk, _shares, _par)
                logger.warning("[paper-exit] %s 거래정지 최초 감지 — 상장주식수=%s 스냅샷 기록",
                                _tk, _shares)
        elif _watch and _watch["resumed_date"] is None and not _watch["resolved"]:
            await record_halt_resumed(db_pool, _tk)
            _shares, _ = await get_listed_shares(db_pool, _tk)
            _before = _watch["listed_shares_before"]
            if _before and _shares and _shares != _before:
                _ratio = _before / _shares
                _hint = (f"상장주식수 {_before:,} → {_shares:,} (비율 추정 ≈{_ratio:.2f}배) — "
                         f"DART 공시로 정확한 비율 확인 후 scripts/resolve_paper_halt.py로 반영")
            else:
                _hint = "상장주식수 변화 없음 — 병합/분할 외 다른 사유일 수 있음, 직접 원인 확인 필요"
            _resume_alerts.append(f"{_tk}: {_hint}")
            logger.warning("[paper-exit] %s 거래정지 재개 감지 — %s", _tk, _hint)

    # 실시간 정지 + 재개됐지만 아직 사람이 확정 안 한 것 전부 스킵 대상
    _unresolved = {h["ticker"] for h in await get_unresolved_halts(db_pool)}
    _halted = _live_halted | _unresolved
    logger.info("[paper-exit] Kiwoom 현재가 조회: %d/%d 종목, 청산판정 스킵(정지 관련): %d종목",
                len(_prices), len(_all_tickers), len(_halted))

    _closed, _tp1_fired, _watermark_updated = 0, 0, 0
    _halted_positions: list[tuple[str, str]] = []  # (ticker, model) — 알림용

    for _pos in _open_positions:
        _pos_id       = _pos["id"]
        _ticker       = _pos["ticker"]
        _entry        = _pos["entry_actual"] or _pos["entry_theory"]
        _hard_stop    = _pos["hard_stop_pct"]
        _tp1_pct      = _pos["tp1_pct"]
        _tp1_ratio    = _pos["tp1_ratio"]
        _trail_pct    = _pos["trail_pct"]
        _tp1_done     = _pos["tp1_date"] is not None
        _watermark    = _pos["watermark"] or _entry
        _signal_date  = _pos["signal_date"]
        _qty          = _pos["qty"] or 0

        if _ticker in _halted:
            logger.info("[paper-exit] %s 거래정지 의심 — 청산 판정 스킵", _ticker)
            _halted_positions.append((_ticker, _pos["model"]))
            continue

        if not _entry or _entry <= 0:
            continue

        # 현재가 룩업 (Kiwoom 실 API 조회 결과)
        _close = _prices.get(_ticker)
        if not _close or _close <= 0:
            logger.warning("[paper-exit] %s 현재가 없음 — 스킵", _ticker)
            continue

        _ret = (_close - _entry) / _entry  # 수익률 (미실현)

        # 워터마크 갱신 (고점 추적)
        if _close > _watermark:
            _watermark = float(_close)
            _watermark_updated += 1
            try:
                async with db_pool.acquire() as _conn:
                    await _conn.execute(
                        "UPDATE paper_positions SET watermark=$1 WHERE id=$2",
                        _watermark, _pos_id,
                    )
            except Exception as _e:
                logger.warning("[paper-exit] %s 워터마크 업데이트 실패: %s", _ticker, _e)

        # ── exit 조건 판정 ────────────────────────────────────
        _exit_type = None
        _blended   = None

        # 1. 최대 보유일 (91일)
        if (today - _signal_date).days >= 91:
            _exit_type = "period_end"

        # 2. 하드 스탑
        elif _close <= _entry * (1 - _hard_stop):
            _exit_type = "hard_stop"

        # 3. 1차 익절 (TP1 미발동 시)
        elif not _tp1_done and _close >= _entry * (1 + _tp1_pct):
            # TP1 발동: 절반 청산 기록만 (잔여분은 계속 보유)
            _exit_type = None   # 전량 청산 아님
            _tp1_fired += 1
            try:
                async with db_pool.acquire() as _conn:
                    await _conn.execute(
                        "UPDATE paper_positions SET tp1_date=$1, tp1_price=$2 WHERE id=$3",
                        today, float(_close), _pos_id,
                    )
                logger.info("[paper-exit] %s TP1 발동 +%.1f%% (잔여분 트레일링 계속)",
                            _ticker, _ret * 100)
            except Exception as _e:
                logger.warning("[paper-exit] %s TP1 기록 실패: %s", _ticker, _e)

        # 4. 트레일링 스탑 (TP1 발동 후)
        elif _tp1_done and _close <= _watermark * (1 - _trail_pct):
            _exit_type = "trail"

        if _exit_type is None:
            continue

        # ── 매도주문 제출 ─────────────────────────────────────
        # 주문 제출 직전 보유수량 스냅샷 — confirm_fill()의 델타 계산 기준.
        _qty_before = await _loop.run_in_executor(
            None, paper_trader.get_position_qty, _ticker
        )
        _sell_ord = ""
        if _qty > 0:
            try:
                _sell_ord = await _loop.run_in_executor(
                    None, paper_trader.place_sell, _ticker, _qty
                )
            except Exception as _e:
                logger.warning("[paper-exit] %s 매도주문 실패: %s", _ticker, _e)
                _sell_ord = "FAILED"

        # 매도주문이 실패하면 브로커에 주식이 그대로 남으므로 청산 확정하지 않고
        # open 상태를 유지 — 다음 실행에서 재시도된다(2026-08-03 investigate 세션에서
        # 발견: 실패해도 무조건 closed 처리해 브로커 실보유와 DB가 어긋나는 버그였음).
        if _sell_ord == "FAILED":
            try:
                async with httpx.AsyncClient() as _http:
                    await _post_message(
                        _http, _get_token(), _get_chat_id(),
                        f"⚠️ 매도주문 실패 — {_ticker} ({_pos['model']}) exit_type={_exit_type}, "
                        f"청산 미확정 (다음 실행에서 재시도)",
                        label="paper-exit", parse_mode=None,
                    )
            except Exception as _e:
                logger.warning("[paper-exit] %s 매도 실패 알림 전송 실패: %s", _ticker, _e)
            continue

        # 주문 접수(ord_no) ≠ 체결 확정 — 보유수량 스냅샷 델타로 실제 체결 수량 확인
        # (ka10076은 이 계좌에서 항상 빈 응답이라 2026-08-05 폐기, get_positions() 비교로 대체).
        # 미체결/부분체결이면 브로커에 주식이 그대로(또는 일부) 남으므로 위 FAILED
        # 분기와 동일하게 청산 미확정 상태로 두고 다음 실행에서 재시도한다.
        _sell_filled = await _loop.run_in_executor(
            None, paper_trader.confirm_fill, _ticker, _sell_ord, _qty, False, _qty_before
        )
        if _sell_filled < _qty:
            logger.warning("[paper-exit] %s 매도 체결 미확인 %d/%d주 (주문번호=%s)",
                            _ticker, _sell_filled, _qty, _sell_ord)
            try:
                async with httpx.AsyncClient() as _http:
                    await _post_message(
                        _http, _get_token(), _get_chat_id(),
                        f"⚠️ 매도 체결 미확인 — {_ticker} ({_pos['model']}) "
                        f"{_sell_filled}/{_qty}주 주문번호={_sell_ord}, "
                        f"청산 미확정 (다음 실행에서 재시도)",
                        label="paper-exit", parse_mode=None,
                    )
            except Exception as _e:
                logger.warning("[paper-exit] %s 미확인 알림 전송 실패: %s", _ticker, _e)
            continue

        # blended_return 계산 (TP1 발동 시 가중평균)
        if _tp1_done:
            _tp1_ret = (_pos["tp1_price"] - _entry) / _entry if _pos.get("tp1_price") else 0
            _final   = _ret
            _blended = _tp1_ratio * _tp1_ret + (1 - _tp1_ratio) * _final
        else:
            _blended = _ret

        try:
            await update_to_closed(db_pool, _pos_id, float(_close), _exit_type, _sell_ord, _blended)
            _closed += 1
            logger.info("[paper-exit] %s 청산 [%s] 수익=%.2f%% 주문번호=%s",
                        _ticker, _exit_type, _blended * 100, _sell_ord)
        except Exception as _e:
            logger.warning("[paper-exit] %s DB 청산 업데이트 실패: %s", _ticker, _e)

    logger.info("[paper-exit] 완료 — 청산=%d TP1발동=%d 워터마크갱신=%d",
                _closed, _tp1_fired, _watermark_updated)

    if _closed > 0:
        try:
            _msg = (
                f"모의투자 청산 완료 ({today})\n"
                f"청산: {_closed}건 | TP1발동: {_tp1_fired}건"
            )
            async with httpx.AsyncClient() as _http:
                await _post_message(_http, _get_token(), _get_chat_id(),
                                    _msg, label="paper-exit", parse_mode=None)
        except Exception as _e:
            logger.warning("[paper-exit] 텔레그램 알림 실패: %s", _e)

    if _halted_positions:
        try:
            _lines = "\n".join(f"  {t} ({m})" for t, m in _halted_positions)
            _msg = (
                f"⚠️ 거래정지 의심 종목 {len(_halted_positions)}건 — 청산 판정 스킵 ({today})\n"
                f"{_lines}\n"
                f"(액면병합 등으로 정지된 경우 재개 후 entry_actual/qty 수동 보정 필요)"
            )
            async with httpx.AsyncClient() as _http:
                await _post_message(_http, _get_token(), _get_chat_id(),
                                    _msg, label="paper-exit", parse_mode=None)
        except Exception as _e:
            logger.warning("[paper-exit] 거래정지 알림 전송 실패: %s", _e)

    if _resume_alerts:
        try:
            _lines = "\n".join(f"  {a}" for a in _resume_alerts)
            _msg = (
                f"🔔 거래정지 재개 감지 {len(_resume_alerts)}건 — 수동 확정 전까지 청산 판정 계속 스킵 ({today})\n"
                f"{_lines}\n"
                f"확정: scripts/resolve_paper_halt.py <티커> --ratio <비율> --apply"
            )
            async with httpx.AsyncClient() as _http:
                await _post_message(_http, _get_token(), _get_chat_id(),
                                    _msg, label="paper-exit", parse_mode=None)
        except Exception as _e:
            logger.warning("[paper-exit] 거래정지 재개 알림 전송 실패: %s", _e)


async def paper_eod_sampler_job(db_pool, paper_trader) -> None:
    """stage_classifier(16:30) 완료 후 — 오늘 신호를 sampling → pending 삽입."""
    today = date.today()
    logger.info("[paper-sampler] EOD 샘플링 시작 (%s)", today)

    # 오늘 Stage 1 진입 종목
    try:
        async with db_pool.acquire() as _conn:
            _stage1_rows = await _conn.fetch(
                "SELECT ticker, s1_high FROM stage_classifications "
                "WHERE classified_date=$1 AND stage=1",
                today,
            )
    except Exception as _e:
        logger.warning("[paper-sampler] stage_classifications 조회 실패: %s", _e)
        return

    stage1_all = [{"ticker": r["ticker"], "price": float(r["s1_high"] or 0)}
                  for r in _stage1_rows]
    logger.info("[paper-sampler] Stage1 %d종목", len(stage1_all))

    # 최신 Ichimoku 통과 종목
    try:
        _week, _ichi_rows = await load_chart_signals_latest(db_pool)
        _ichi_tickers = {r["ticker"] for r in _ichi_rows}
        _ichi_price   = {r["ticker"]: float(r.get("close") or 0) for r in _ichi_rows}
    except Exception as _e:
        logger.warning("[paper-sampler] Ichimoku 조회 실패: %s", _e)
        _ichi_tickers, _ichi_price = set(), {}

    # Cross = Stage1 ∩ Ichimoku
    cross_signals  = [s for s in stage1_all if s["ticker"] in _ichi_tickers]
    stage_kospi    = [s for s in stage1_all if s["ticker"].endswith(".KS")]
    stage_kosdaq   = [s for s in stage1_all if s["ticker"].endswith(".KQ")]
    ichi_signals   = [{"ticker": t, "price": _ichi_price.get(t, 0)}
                      for t in _ichi_tickers]

    model_queue = [
        ("stage",    stage_kospi,   _KOSPI_P),
        ("kosdaq",   stage_kosdaq,  _KOSDAQ_P),
        ("cross",    cross_signals, _CROSS_P),
        ("ichimoku", ichi_signals,  _ICHI_P),
    ]

    total_inserted = 0
    seed_base = int(today.strftime("%Y%m%d"))

    for _model, _signals, _params in model_queue:
        if _model not in ACTIVE_MODELS:
            # 자본배분 대상(ACTIVE_MODELS) 밖 모델 — 예: kosdaq. 여기서 걸러야
            # paper_open_entry_job에서 영원히 pending으로 남는 후보가 애초에
            # 생기지 않는다 (2026-08-22 code-review 발견: 144/145번 pending이
            # 이 가드 부재로 8/20·8/21에 각각 생성돼 계속 스킵되고 있었음).
            if _signals:
                logger.info("[paper-sampler] [%s] 자본배분 대상 아님 — %d건 후보 스킵",
                            _model, len(_signals))
            continue
        if not _signals:
            continue
        _cfg       = MODEL_CONFIG.get(_model, {"max_slots": 10})
        _open_cnt  = await get_open_slot_count(db_pool, _model)
        _available = _cfg["max_slots"] - _open_cnt
        if _available <= 0:
            logger.info("[paper-sampler] [%s] 슬롯 없음 (%d/%d)", _model, _open_cnt, _cfg["max_slots"])
            continue

        # 이미 이 모델이 open/pending으로 보유 중인 티커는 후보에서 제외 —
        # 그대로 두면 같은 티커에 포지션이 중복으로 열려 브로커 잔고가 모델별로
        # 분리되지 않고, 이후 exit 재조정도 안전하게 못 하게 된다(241710.KQ 사례).
        _held = await get_open_or_pending_tickers(db_pool, _model)
        _candidates = [s for s in _signals if s["ticker"] not in _held]
        if len(_candidates) < len(_signals):
            logger.info("[paper-sampler] [%s] 이미 보유 중인 티커 %d건 제외",
                        _model, len(_signals) - len(_candidates))
        if not _candidates:
            continue

        random.seed(seed_base ^ (hash(_model) & 0xFFFFFFFF))
        _selected = random.sample(_candidates, min(_available, len(_candidates)))

        for _sig in _selected:
            try:
                _pid = await insert_pending(
                    db_pool,
                    model=_model,
                    ticker=_sig["ticker"],
                    signal_date=today,
                    entry_theory=_sig["price"],
                    tp1_pct=_params.get("tp1_pct", 0.15),
                    tp1_ratio=_params.get("tp1_ratio", 0.50),
                    trail_pct=_params.get("trail_pct", 0.10),
                    hard_stop_pct=_params.get("hard_stop_pct", 0.10),
                )
                total_inserted += 1
                logger.info("[paper-sampler] [%s] %s pending (id=%d, theory=%.0f)",
                            _model, _sig["ticker"], _pid, _sig["price"])
            except Exception as _e:
                logger.warning("[paper-sampler] [%s] %s 삽입 실패: %s", _model, _sig["ticker"], _e)

    logger.info("[paper-sampler] 완료 — %d건 pending 삽입", total_inserted)

    if total_inserted > 0:
        try:
            _kosdaq_note = (
                " (자본배분 대상 아님 — 전부 스킵)"
                if stage_kosdaq and "kosdaq" not in ACTIVE_MODELS else ""
            )
            _msg = (
                f"모의투자 EOD 샘플링 ({today})\n"
                f"Stage KOSPI {len(stage_kospi)}건 / KOSDAQ {len(stage_kosdaq)}건{_kosdaq_note} / "
                f"Cross {len(cross_signals)}건 / Ichimoku {len(ichi_signals)}건\n"
                f"→ pending 삽입: {total_inserted}건"
            )
            async with httpx.AsyncClient() as _http:
                await _post_message(_http, _get_token(), _get_chat_id(),
                                    _msg, label="paper-sampler", parse_mode=None)
        except Exception as _e:
            logger.warning("[paper-sampler] 텔레그램 알림 실패: %s", _e)


async def paper_open_entry_job(db_pool, paper_trader) -> None:
    """장 시작(09:00) 후 시가 확정 → pending 포지션 매수주문 제출.

    슬롯당 매수 금액은 실행 시점 계좌 자산 기준으로 매번 다시 계산한다
    (compute_slot_krw) — 모델별로 동일 금액을 배분하고, 현금 비중(CASH_RESERVE_RATIO)
    만큼은 신규 진입에서 제외한다. 이번 실행에서 이미 낸 주문의 매입금액도
    누적 반영해, 한 번에 여러 건을 살 때 배포 가능 자본을 넘기지 않도록 한다.
    """
    _loop = asyncio.get_running_loop()

    # 오래 방치된 pending(신호일로부터 STALE_PENDING_MAX_AGE_DAYS 경과)은 이번
    # 재시도 대상에서 먼저 걷어내 held로 전환 — 안 그러면 가격조회 실패나
    # ACTIVE_MODELS 제외 같은 사유로 영원히 재시도만 반복되는 pending이 쌓인다
    # (2026-08-22 adversarial review 발견: kosdaq 144/145번 pending 사례,
    # 당시엔 수동 SQL로 처리 — 이제 자동화).
    _n_held = await mark_stale_pending_as_held(db_pool)
    if _n_held:
        logger.info("[paper-entry] 신호일 초과 pending %d건 held로 전환", _n_held)

    # signal_date 무관하게 미체결 pending 전체 처리 (과거 실패분 포함 재시도)
    _pending = await get_pending_positions(db_pool)
    if not _pending:
        logger.info("[paper-entry] pending 없음")
        return

    _balance   = await _loop.run_in_executor(None, paper_trader.get_balance)
    _slot_krw  = compute_slot_krw(_balance)
    _deployable = deployable_capital(_balance)
    _invested   = _balance["tot_pur_amt"]

    logger.info("[paper-entry] %d건 pending → 매수주문 시작 (배포가능자본=%.0f, 기투자=%.0f)",
                len(_pending), _deployable, _invested)

    _inactive_model_skips: dict[str, int] = defaultdict(int)  # 로그 스팸 방지용 집계

    for _pos in _pending:
        _ticker = _pos["ticker"]
        _model  = _pos["model"]
        _pos_id = _pos["id"]

        if _model not in _slot_krw:
            # ACTIVE_MODELS(자본배분 대상)에 없는 모델 — 예: kosdaq은 신호 생성
            # 버그로 후보가 없다는 전제하에 제외돼 있었으나(2026-07-29), 그 전제가
            # 깨지고 후보가 들어오면 예전엔 10,000,000원 기본값이 적용돼 다른
            # 모델 슬롯(수십만원)의 10배 이상 금액으로 진입하는 버그가 있었다
            # (2026-08-20 발견). ACTIVE_MODELS 밖 모델은 자금 배정이 없다는 뜻이므로
            # 스킵하고 pending 유지 — 다음 실행에서 재시도(가격조회 실패 스킵과 동일 패턴).
            # _slot_krw만으로 판별 가능하므로 가격 조회(0.5초 딜레이 + 실 API 호출)보다
            # 먼저 체크해 불필요한 API 소모를 막는다(2026-08-22 code-review 발견).
            # 건별 warning 대신 모델별로 집계해 루프 종료 후 한 번만 남긴다 —
            # 이미 쌓인 pending이 매 실행마다 같은 경고를 반복 찍어 로그를
            # 잠식하는 걸 막기 위함(2026-08-22 adversarial review 발견).
            _inactive_model_skips[_model] += 1
            logger.debug("[paper-entry] %s 모델 '%s'는 자본배분 대상 아님 — 스킵", _ticker, _model)
            continue

        # 시가 조회 (ka10001 open_pric, 실 API) — 종목 간 0.5초 딜레이로 rate limit 방지
        await asyncio.sleep(0.5)
        _open_px = await _loop.run_in_executor(None, paper_trader.get_open_price, _ticker)
        if not _open_px:
            _open_px = await _loop.run_in_executor(None, paper_trader.get_current_price, _ticker)
        if not _open_px:
            logger.warning("[paper-entry] %s 가격 조회 실패 — 스킵", _ticker)
            continue

        _slot_amount = _slot_krw[_model]
        _qty = _qty_from_price(_slot_amount, _open_px)
        if _qty <= 0:
            logger.warning("[paper-entry] %s qty=0 (price=%d) — 스킵", _ticker, _open_px)
            continue

        _order_cost = _qty * _open_px
        if _invested + _order_cost > _deployable:
            logger.info(
                "[paper-entry] %s 현금 비중 보호 — 배포가능자본 초과로 스킵 "
                "(기투자=%.0f + 주문=%.0f > 한도=%.0f)",
                _ticker, _invested, _order_cost, _deployable,
            )
            continue

        # 매수주문 제출 직전 보유수량 스냅샷 — confirm_fill()의 델타 계산 기준.
        _qty_before = await _loop.run_in_executor(
            None, paper_trader.get_position_qty, _ticker
        )
        try:
            _ord_no = await _loop.run_in_executor(
                None, paper_trader.place_buy, _ticker, _qty
            )
        except Exception as _e:
            logger.warning("[paper-entry] %s 매수주문 실패: %s", _ticker, _e)
            continue

        # 주문 접수(ord_no) ≠ 체결 확정 — 보유수량 스냅샷 델타로 실제 체결 수량 확인
        # (ka10076은 이 계좌에서 항상 빈 응답이라 2026-08-05 폐기, get_positions() 비교로 대체.
        #  2026-08-03 investigate: 애초에 접수만 되고 미체결인 주문이 조용히 성공 처리된 사고가 계기).
        _filled = await _loop.run_in_executor(
            None, paper_trader.confirm_fill, _ticker, _ord_no, _qty, True, _qty_before
        )

        if _filled <= 0:
            # 2026-08-13 발견(003010.KS 등): confirm_fill()의 폴링창(5회×3초)이 이
            # 모의투자 서버의 실제 체결 지연(수 분~10분+)보다 훨씬 짧은 건 매도쪽과
            # 동일 — 그런데 매수쪽은 여기서 즉시 buy_never_filled로 영구 종결시켜서
            # 나중에 실제로 체결됐다는 게 밝혀져도 브로커 실보유와 대조할 열린 행이
            # DB에 아예 없어져(status='closed') 유령 보유가 반복 재발했다(과거 20건과
            # 동일 근본 원인). 매도쪽처럼 즉시 확정 대신 open(qty=0)으로 보류 — 다음
            # exit-checker의 _reconcile_stale_positions()가 브로커 실보유 기준으로
            # 최종 판정한다(qty=0 그대로면 진짜 미체결 확정 closed, qty>0 확인되면
            # 평균단가로 open 채움).
            logger.warning("[paper-entry] %s 매수 체결 미확인 (주문번호=%s) — open(qty=0)로 보류, "
                            "다음 실행 재조정에서 최종 판정", _ticker, _ord_no)
            try:
                await update_to_open(db_pool, _pos_id, float(_open_px), 0, _ord_no,
                                      qty_ordered=_qty)
            except Exception as _e:
                logger.warning("[paper-entry] %s DB 업데이트 실패: %s", _ticker, _e)
            try:
                async with httpx.AsyncClient() as _http:
                    await _post_message(
                        _http, _get_token(), _get_chat_id(),
                        f"⚠️ 매수 체결 미확인 — {_ticker} ({_model}) 주문번호={_ord_no}, "
                        f"다음 실행 재조정에서 최종 판정 (open qty=0 보류)",
                        label="paper-entry", parse_mode=None,
                    )
            except Exception as _e:
                logger.warning("[paper-entry] %s 미확인 알림 전송 실패: %s", _ticker, _e)
            continue

        if _filled < _qty:
            logger.warning("[paper-entry] %s 부분체결 %d/%d주 (주문번호=%s)",
                            _ticker, _filled, _qty, _ord_no)
            try:
                async with httpx.AsyncClient() as _http:
                    await _post_message(
                        _http, _get_token(), _get_chat_id(),
                        f"⚠️ 매수 부분체결 — {_ticker} ({_model}) {_filled}/{_qty}주 "
                        f"주문번호={_ord_no}",
                        label="paper-entry", parse_mode=None,
                    )
            except Exception as _e:
                logger.warning("[paper-entry] %s 부분체결 알림 전송 실패: %s", _ticker, _e)

        _invested += _filled * _open_px  # 같은 실행 내 후속 주문의 한도 판정에 실체결 기준 반영

        # pending → open (실제 체결 수량 기준)
        try:
            await update_to_open(db_pool, _pos_id, float(_open_px), _filled, _ord_no,
                                  qty_ordered=_qty)
            logger.info("[paper-entry] %s %d주 매수 완료 (시가=%d, 주문번호=%s)",
                        _ticker, _filled, _open_px, _ord_no)
        except Exception as _e:
            logger.warning("[paper-entry] %s DB 업데이트 실패: %s", _ticker, _e)

    if _inactive_model_skips:
        _summary = ", ".join(f"{m}={n}건" for m, n in _inactive_model_skips.items())
        logger.warning(
            "[paper-entry] 자본배분 대상(ACTIVE_MODELS) 아닌 모델 pending 스킵: %s "
            "(전부 다음 실행에서 재시도 — 계속 반복되면 수동 확인 필요)",
            _summary,
        )

    logger.info("[paper-entry] 완료")
