"""
resolve_paper_halt.py — 액면병합/분할로 거래정지됐던 종목의 재개 후 수동 보정.

배경 (2026-09-12): 208860.KQ(다산디엠씨)가 액면병합으로 09-02부터 매매거래
정지된 걸 발견 — Kiwoom ka10001엔 정지 전용 필드가 없어 paper_exit_checker_job이
trde_qty/open_pric=0으로 근사 감지하고(is_trading_halted), 정지 감지 시점의
krx_listings.listed_shares를 paper_halt_watch에 스냅샷해둔다. 재개가 감지되면
그 스냅샷과 재개 시점 listed_shares를 비교해 비율을 "추정"만 하고 텔레그램으로
알릴 뿐, entry_actual/qty는 자동으로 건드리지 않는다 — 병합비율을 코드가
확신 없이 추측해서 적용하면 gen1 문서가 이미 경고한 "근거 없는 근사치로 실전
성과 통계를 오염시키는" 사고를 반복하게 된다.

이 스크립트는 사람이 DART 공시 등으로 정확한 병합/분할 비율을 확인한 뒤 실행하는
수동 확정 단계다:
  1. 대상 티커의 status='open' 포지션 전부(모델 무관) 조회.
  2. entry_actual/entry_theory/watermark엔 ratio를 곱하고(가격 상승분 반영),
     qty는 ratio로 나눈다(주식수 감소분 반영) — 병합 비율 R:1이면 가격은 R배,
     수량은 1/R배가 되는 표준적인 관계.
  3. paper_halt_watch.resolved=TRUE로 표시 — 이후 exit-checker가 다시 정상
     청산 판정 대상에 포함시킨다.

비율 방향에 주의: "5:1 병합"이면 ratio=5 (가격 5배, 수량 1/5). 분할이면
ratio를 1 미만 소수로 넣는다(예: 1:2 분할 → ratio=0.5, 가격 절반·수량 2배).

사용법:
  python scripts/resolve_paper_halt.py --ticker 208860.KQ --ratio 5          # dry-run
  python scripts/resolve_paper_halt.py --ticker 208860.KQ --ratio 5 --apply  # 실제 반영
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.db as _db  # noqa: E402


async def _load_open_rows(pool, ticker: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM paper_positions WHERE ticker=$1 AND status='open' ORDER BY model",
            ticker,
        )
    return [dict(r) for r in rows]


async def resolve(pool, ticker: str, ratio: float, apply: bool) -> None:
    from data.kiwoom_paper_trader import get_halt_watch

    watch = await get_halt_watch(pool, ticker)
    if not watch:
        print(f"[{ticker}] paper_halt_watch에 감시 기록 없음 — "
              f"is_trading_halted가 이 티커를 정지로 감지한 적이 없다는 뜻입니다. "
              f"그래도 계속 진행하려면 이 경고를 무시하세요.")
    elif watch["resolved"]:
        print(f"[{ticker}] 이미 resolved=TRUE 상태 — 다시 확정할 필요 없음(재실행하려면 DB에서 직접 리셋).")
        return

    rows = await _load_open_rows(pool, ticker)
    if not rows:
        print(f"[{ticker}] open 포지션 없음 — 그래도 paper_halt_watch만 resolved 처리합니다.")
    else:
        print(f"[{ticker}] open {len(rows)}건, ratio={ratio} 적용 계획:")
        for r in rows:
            old_entry = r["entry_actual"] or r["entry_theory"]
            old_qty = r["qty"] or 0
            old_wm = r["watermark"]
            new_entry = old_entry * ratio if old_entry else old_entry
            new_qty = math.floor(old_qty / ratio) if old_qty else old_qty
            new_wm = old_wm * ratio if old_wm else old_wm
            print(f"  id={r['id']} model={r['model']}: "
                  f"entry {old_entry}→{new_entry}, qty {old_qty}→{new_qty}, "
                  f"watermark {old_wm}→{new_wm}")

    if not apply:
        print("  (dry-run — 실제로 반영하려면 --apply)")
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                old_entry = r["entry_actual"] or r["entry_theory"]
                old_qty = r["qty"] or 0
                old_wm = r["watermark"]
                new_entry = old_entry * ratio if old_entry else old_entry
                new_qty = math.floor(old_qty / ratio) if old_qty else old_qty
                new_wm = old_wm * ratio if old_wm else old_wm
                await conn.execute(
                    "UPDATE paper_positions SET entry_actual=$1, qty=$2, watermark=$3 WHERE id=$4",
                    new_entry, new_qty, new_wm, r["id"],
                )
            await conn.execute(
                """
                INSERT INTO paper_halt_watch (ticker, detected_date, resolved)
                VALUES ($1, CURRENT_DATE, TRUE)
                ON CONFLICT (ticker) DO UPDATE SET resolved=TRUE, updated_at=NOW()
                """,
                ticker,
            )

    print(f"  반영 완료 — {ticker} 다음 exit-checker 실행부터 정상 청산 판정 대상에 포함됩니다.")


async def main() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔에서 한글 로그 깨짐 방지

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else "")
    parser.add_argument("--ticker", required=True, help="대상 티커 (예: 208860.KQ)")
    parser.add_argument("--ratio", required=True, type=float,
                         help="병합/분할 비율 — R:1 병합이면 R, 1:R 분할이면 1/R (예: 5:1 병합 → 5)")
    parser.add_argument("--apply", action="store_true",
                         help="실제로 DB 반영 (기본은 dry-run — 계획만 출력, 변경 없음)")
    args = parser.parse_args()

    if args.ratio <= 0:
        print("--ratio는 0보다 커야 합니다.")
        sys.exit(1)

    pool = await _db.create_pool()
    try:
        await resolve(pool, args.ticker, args.ratio, args.apply)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
