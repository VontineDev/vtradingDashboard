"""
krx_flow_sync.py — 외국인/기관 순매수 이력을 daily_flow 테이블에 적재

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
백엔드 (--backend 옵션으로 선택, 기본: krx-direct):

  krx-direct (기본, 권장)
    data.krx.co.kr에 직접 HTTP 요청. pykrx 의존성 없음.
    Python 3.14에서도 동작 (pykrx EUC-KR 파싱 버그 없음).
    .env에 KRX_ID=아이디, KRX_PW=비밀번호 필요.
    한국 ISP 또는 VPN 환경 필요 (해외 IP 차단).

  pykrx
    pykrx 라이브러리 경유. Python 3.12 이하 권장.
    동일 자격증명(KRX_ID/KRX_PW) 사용.

  csv
    --csv <파일> 로 수동 임포트.
    data.krx.co.kr → 주식 → 투자자별 거래실적 → CSV 다운로드

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
참고: KRX OpenAPI (openapi.krx.co.kr / pykrx-openapi)는
      투자자 순매수 데이터를 제공하지 않습니다 (OHLCV/지수만 제공).
      투자자 데이터는 data.krx.co.kr에서만 제공됩니다.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

사용법:
  # 벌크 적재 (초기 1회):
  python krx_flow_sync.py --start 2023-01-01 --end 2026-05-03

  # 특정 백엔드:
  python krx_flow_sync.py --start 2025-01-01 --backend pykrx
  python krx_flow_sync.py --csv /path/to/data.csv --backend csv

  # 응답 구조 확인 (첫 실행 시 권장):
  python krx_flow_sync.py --probe 005930

  # 증분 모드 (스케줄러에서 매일):
  python krx_flow_sync.py --incremental

  # 시장/티커 수 제한 (테스트):
  python krx_flow_sync.py --start 2025-01-01 --end 2026-05-03 --market KOSPI --max 50
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CSV 형식 (--csv):
  컬럼: date, ticker, foreign_net, inst_net
  date 형식: YYYY-MM-DD 또는 YYYYMMDD
  ticker: yfinance 심볼 (005930.KS) 또는 KRX 6자리 코드 (005930)
  예:
    date,ticker,foreign_net,inst_net
    2025-01-02,005930.KS,123456,-78900
    2025-01-02,035720.KQ,-5000,12000

  또는 KRX 다운로드 CSV (자동 감지):
    날짜,종목코드,종목명,외국인순매수,기관합계
    20250102,005930,삼성전자,123456,-78900
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json as _json
import logging
import os
import random
import sys
import time as _time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from data.kiwoom_aftermarket_sync import KiwoomClient

from core.dates import last_trading_day
from core.env import load_env_once, refresh_env
from core.tor import jittered_delay, new_identity

load_env_once()
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── 수급 레코드 ────────────────────────────────────────────────


@dataclass
class FlowRecord:
    ticker: str        # yfinance 심볼 (005930.KS / 035720.KQ)
    trade_date: date
    foreign_net: Optional[int]   # 외국인 순매수 (주). 음수 = 순매도
    inst_net: Optional[int]      # 기관 순매수 (주). 음수 = 순매도
    foreign_streak: Optional[int] = None
    inst_streak: Optional[int] = None
    personal_net: Optional[int] = None     # 개인 순매수 (주). 음수 = 순매도
    personal_streak: Optional[int] = None  # 개인 연속 순매수일


# ── 스트릭 계산 ────────────────────────────────────────────────

def _next_streak(prev: Optional[int], net: Optional[int]) -> Optional[int]:
    """순매수 연속일 계산.
    prev=None → 초기값으로 취급 (net 부호만 반영).
    net=None  → 데이터 없음, streak 0 반환.
    """
    if net is None:
        return 0
    p = prev or 0
    if net > 0:
        return max(1, p + 1)
    if net < 0:
        return min(-1, p - 1)
    return 0


def _isin_from_krx_code(krx_code: str) -> str:
    """KRX 6자리 코드 → 12자리 ISIN.

    MDCSTAT02302는 isuSrtCd(6자리) 단독으로는 output=[] 반환.
    isuCd(ISIN)가 반드시 필요.

    ISIN의 type digit(3번째 자리)은 상장 시장(KOSPI/KOSDAQ)이 아니라
    증권 종류(보통주=7)로 결정된다 — KOSDAQ 보통주도 KR7을 쓴다.
    (실측: KR8247540006 조회 시 output=[] 빈 응답, KR7247540008로
    동일 종목 조회 시 정상 데이터 수신. tests/test_krx_sync.py의
    KOSDAQ 픽스처(086520 에코프로비엠)도 ISU_CD="KR7086520006"로
    이미 KR7을 사용 중이었음 — 이 함수만 잘못된 KR8 가정을 갖고 있었다.)
    이전엔 suffix==".KS"가 아니면 KR8을 써서 KOSDAQ 종목의 수급 조회가
    전부 빈 응답으로 조용히 실패했다(daily_flow에 KOSDAQ 행이 전무했던 원인).
    """
    type_digit = "7"
    body = f"KR{type_digit}{krx_code}00"  # 11자리
    digits = "".join(str(ord(c) - 55) if c.isalpha() else c for c in body)
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = int(d)
        if i % 2 == 0:
            n = n * 2
            if n > 9:
                n -= 9
        total += n
    return body + str((10 - total % 10) % 10)


# ── KRX Direct 백엔드 ─────────────────────────────────────────
#
# pykrx 없이 data.krx.co.kr에 직접 HTTP 요청.
# pykrx의 Python 3.14 EUC-KR 파싱 버그를 우회하며,
# 동일한 KRX_ID/KRX_PW 자격증명을 사용합니다.
#
# data.krx.co.kr 투자자 거래실적 엔드포인트:
#   bld=dbms/MDC/STAT/standard/MDCSTAT02302  (일자별 투자자 거래실적)
#   isuSrtCd=005930  (6자리 종목코드)
#   strtDd/endDd=YYYYMMDD
#   inqTpCd=1 (순매수 수량 기준)
#   trdVolVal=1 (거래량)
#   askBid=3 (순매수=매수-매도)
#
# 응답 구조 (행당 투자자 유형 1개):
#   INVST_TP_NM: "외국인합계" | "기관합계" | "금융투자" | ...
#   TDD_STR_DD:  날짜 YYYYMMDD
#   NETBID_TRDVOL: 순매수 주수 (음수=순매도)
#
# --probe <종목코드> 옵션으로 실제 응답 구조를 먼저 확인하세요.


class KrxTerminalAuthError(Exception):
    """재시도/세션갱신 대기로는 절대 복구되지 않는 KRX 로그인 실패.

    예: CD010(비밀번호 변경 필요), CD007(로그인 시도 초과로 인한 잠금) —
    둘 다 세션 쿠키 문제가 아니라 계정 자체가 data.krx.co.kr에서 막힌
    상태라 사람이 직접 확인/조치해야 풀린다. 2026-09-07 CD010을 CD003과
    동일한 일반 실패로 취급해서 세션-만료 오판 → 30분 수동 갱신 대기 →
    Tor 회로 로테이션까지 총 2시간 26분을 낭비하고서야 exit(1)한 사고로
    추가했고, 2026-09-09 같은 계정이 CD007로도 잠기는 걸 실측 — 코드가
    하나가 아니라 "계정이 막히는 여러 원인" 부류라는 게 확인돼 세트로 관리.
    """

    def __init__(self, err_code: Optional[str], err_msg: str) -> None:
        self.err_code = err_code
        self.err_msg = err_msg
        super().__init__(f"[{err_code}] {err_msg}")


# 재시도해도 절대 안 풀리는 로그인 에러 코드 — 발견되는 대로 여기 추가.
# CD010: "패스워드 변경 필요", CD007: "패스워드 오류수에 의한 잠금".
# 둘 다 data.krx.co.kr에서 사람이 직접 확인/조치해야 풀린다.
_TERMINAL_LOGIN_ERROR_CODES = {"CD010", "CD007"}


class _KrxDirectFetcher:
    """pykrx 없이 data.krx.co.kr를 직접 호출하는 수급 수집기."""

    _BASE      = "https://data.krx.co.kr"
    _WARMUP    = _BASE + "/contents/MDC/MDI/index.cmd"
    _LOGIN     = _BASE + "/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    _DATA      = _BASE + "/comm/bldAttendant/getJsonData.cmd"
    _BLD_DAILY = "dbms/MDC/STAT/standard/MDCSTAT02302"  # 일자별 투자자 거래실적

    _HEADERS = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":         _BASE + "/contents/MDC/MDI/outerLoader/index.cmd",
        "X-Requested-With": "XMLHttpRequest",
        "Accept":          "application/json, text/javascript, */*; q=0.01",
        "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
    }

    # isuCd 기반 쿼리의 TRDVAL 컬럼 인덱스 (실증 확인: 2026-04 tariff shock 기준)
    # TRDVAL1=외국인합계, TRDVAL2=기관합계, TRDVAL3=기타법인, TRDVAL4=개인
    _FOREIGN_COL   = "TRDVAL1"
    _INST_COL      = "TRDVAL2"
    _PERSONAL_COL  = "TRDVAL4"
    # KRX API 날짜 범위 상한 (초과 시 400). 30일은 OK, 실제 상한 미확인 → 보수적 90일.
    _CHUNK_DAYS  = 90

    def __init__(self) -> None:
        import requests as _req
        self._session = _req.Session()
        self._authenticated = False
        # 세션 갱신 대기(_handle_possible_expiry)가 타임아웃되면 세워짐 —
        # 이후 남은 종목에 대해 대기 루프 재진입을 막는 플래그.
        self._session_wait_exhausted = False
        # LOGOUT 응답 WARNING을 실행당 1회만 남기기 위한 플래그 (종목마다 반복 방지).
        self._logout_warned = False
        # login() 마지막 실패 원인 — 호출부가 재시도 가능 여부를 판단하는 데 사용.
        self.last_error_code: Optional[str] = None
        self.last_error_message: str = ""
        tor_proxy = os.environ.get("TOR_PROXY", "")
        if tor_proxy:
            self._session.proxies = {"http": tor_proxy, "https": tor_proxy}
            logger.info("[krx-direct] Tor 프록시 적용: %s", tor_proxy)

    def _post(self, url: str, data: dict, timeout: int = 30) -> bytes:
        resp = self._session.post(url, data=data, headers=self._HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _decode(raw: bytes) -> str:
        """KRX 응답 디코딩. JSON API는 UTF-8, 구형 페이지는 EUC-KR."""
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("euc-kr", errors="replace")

    def warmup(self) -> bool:
        """KRX 세션 초기화 (JSESSIONID 쿠키 획득)."""
        try:
            self._session.get(self._WARMUP, headers=self._HEADERS, timeout=15)
            return True
        except Exception as e:
            logger.debug("[krx-direct] warmup 실패: %s", e)
            return False

    def login(self, krx_id: str, krx_pw: str) -> bool:
        """KRX 계정 로그인. 성공 시 True.

        data.krx.co.kr 회원 자격증명 필요 (openapi.krx.co.kr과 별도 가입).
        """
        try:
            raw = self._post(self._LOGIN, {
                "mbrNm": "", "telNo": "", "di": "", "certType": "",
                "mbrId": krx_id, "pw": krx_pw,
            })
            text = self._decode(raw)

            # KRX 로그인 성공 응답: "success" 포함 또는 단독 "OK" 응답.
            if "success" in text.lower() or text.strip().upper() == "OK":
                self._authenticated = True
                self.last_error_code = None
                self.last_error_message = ""
                logger.info("[krx-direct] 로그인 성공")
                return True

            err_code, err_msg = self._parse_krx_error(text)

            # CD001("정상")도 성공 응답이다 — 실측(2026-07-11)으로 확인:
            # {"_error_code":"CD001","_error_message":"정상"}. 이전까지는
            # --probe-login이 매번 CD003/CD011(실패)만 만나서 이 케이스가
            # 드러나지 않았고, 실제 성공 응답이 위 "success"/"OK" 체크에
            # 안 걸려 성공한 로그인을 실패로 오판정하고 있었다.
            if err_code == "CD001":
                self._authenticated = True
                self.last_error_code = None
                self.last_error_message = ""
                logger.info("[krx-direct] 로그인 성공 [CD001]")
                return True

            # 실패 — 호출부(_make_krx_direct/_handle_possible_expiry)가 재시도
            # 가능 여부를 판단할 수 있게 원인을 기록해둔다.
            self.last_error_code = err_code
            self.last_error_message = err_msg

            # 실패: KRX 실제 에러 메시지 추출해서 표시
            # CD003("서비스 에러")은 KRX의 범용 인증 실패 코드
            # (잘못된 자격증명, 미가입 계정 모두 동일 코드 반환)
            if err_code == "CD003":
                logger.warning(
                    "[krx-direct] 로그인 실패 [CD003] — "
                    "data.krx.co.kr에 직접 접속해서 KRX_ID/KRX_PW로 로그인 가능한지 확인하세요. "
                    "openapi.krx.co.kr 계정과 data.krx.co.kr 계정은 별도 가입이 필요합니다."
                )
            elif err_code in _TERMINAL_LOGIN_ERROR_CODES:
                logger.warning(
                    "[krx-direct] 로그인 실패 [%s]: %s — 세션 만료가 아니라 계정 자체가 "
                    "막혀있는 상태(재시도/세션갱신으로 복구 불가). data.krx.co.kr에서 "
                    "직접 조치 필요.", err_code, err_msg,
                )
            else:
                logger.warning("[krx-direct] 로그인 실패 [%s]: %s", err_code or "?", err_msg)
            return False
        except Exception as e:
            logger.warning("[krx-direct] 로그인 실패: %s", e)
            self.last_error_code = None
            self.last_error_message = str(e)
            return False

    def inject_session(self, jsessionid: str, visitor_id: Optional[str] = None) -> None:
        """브라우저 쿠키 주입. login() 없이 인증 우회.

        JSESSIONID를 서브도메인 포함(wildcard)과 정확한 도메인 두 곳에 모두 설정.
        requests cookiejar는 더 구체적인 도메인 쿠키를 우선 전송하므로
        warmup 응답이 data.krx.co.kr 범위로 JSESSIONID를 덮어써도
        data.krx.co.kr 범위로 재설정한 쿠키가 이를 이긴다.
        """
        for _domain in (".krx.co.kr", "data.krx.co.kr"):
            self._session.cookies.set("JSESSIONID", jsessionid, domain=_domain)
        self._session.cookies.set("mdc.client_session", "true", domain="data.krx.co.kr")
        if visitor_id:
            self._session.cookies.set("__smVisitorID", visitor_id, domain="data.krx.co.kr")
        self._authenticated = True
        logger.info("[krx-direct] KRX_SESSION 쿠키 주입 완료")

    @staticmethod
    def _parse_krx_error(text: str) -> tuple[str, str]:
        """KRX JSON 에러 응답에서 (_error_code, _error_message) 추출."""
        try:
            err = _json.loads(text)
            return (
                str(err.get("_error_code") or ""),
                str(err.get("_error_message") or text[:120]),
            )
        except Exception:
            return ("", text[:120])

    def fetch_raw(
        self,
        krx_code: str,
        start: date,
        end: date,
        isuCd: Optional[str] = None,
    ) -> list[dict]:
        """단일 종목 투자자별 거래실적 원본 행 목록 반환.

        isuCd(ISIN) 필수: 없으면 output=[] 반환됨.
        반환 형식: [{"TRD_DD": "2026/05/06", "TRDVAL1": "12345", "TRDVAL2": "-678", ...}, ...]
        """
        payload = {
            "bld":       self._BLD_DAILY,
            "isuSrtCd": krx_code,
            "strtDd":   start.strftime("%Y%m%d"),
            "endDd":    end.strftime("%Y%m%d"),
            "inqTpCd":  "1",
            "trdVolVal": "1",
            "askBid":   "3",
            "csvxls_isNo": "false",
        }
        if isuCd:
            payload["isuCd"] = isuCd
        try:
            raw = self._post(self._DATA, payload)
        except Exception as e:
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", None)
            # KRX는 세션 만료(LOGOUT) 응답을 200이 아닌 400으로도 반환한다 —
            # raise_for_status()가 먼저 예외를 던져버려서, 아래 "raw in (b'LOGOUT', b'')"
            # 검사가 이 케이스에는 절대 도달하지 못하고 매 종목 DEBUG로만 조용히
            # 묻혀 세션 만료를 알아채기 어려웠다. 예외에 담긴 응답 바디를 직접 확인.
            body = getattr(resp, "content", b"") if resp is not None else b""
            if status == 403 and new_identity():
                logger.info("[krx-direct] 403 감지 — Tor 새 회로 요청 후 재시도 (%s)", krx_code)
                _time.sleep(3)  # 새 회로 안정화 대기
                try:
                    raw = self._post(self._DATA, payload)
                except Exception as e2:
                    logger.debug("[krx-direct] %s 재시도 실패: %s", krx_code, e2)
                    return []
            elif resp is not None and body in (b"LOGOUT", b""):
                if not self._logout_warned:
                    logger.warning(
                        "[krx-direct] 인증 필요(세션 만료 의심, status=%s) — "
                        "KRX_SESSION 갱신 필요 (이후 동일 실패는 조용히 처리)",
                        status,
                    )
                    self._logout_warned = True
                return []
            else:
                logger.debug("[krx-direct] %s 요청 실패: %s", krx_code, e)
                return []

        # 응답이 LOGOUT/빈값이면 인증 실패
        if raw in (b"LOGOUT", b""):
            logger.warning("[krx-direct] 인증 필요 — KRX_ID/KRX_PW를 확인하세요.")
            return []

        try:
            text = self._decode(raw)
            data = _json.loads(text)
        except Exception as e:
            logger.debug("[krx-direct] JSON 파싱 실패 (%s): %s", krx_code, e)
            return []

        # KRX 응답 키: "output" 또는 "OutBlock_1"
        rows = data.get("output") or data.get("OutBlock_1") or []
        return rows if isinstance(rows, list) else []

    def fetch_records(
        self,
        yf_symbol: str,
        start: date,
        end: date,
        prev_f_streak: Optional[int],
        prev_i_streak: Optional[int],
        prev_p_streak: Optional[int] = None,
        rate_delay: float = 0.5,
    ) -> list[FlowRecord]:
        """단일 종목 수급 FlowRecord 목록 반환.

        KRX API 날짜 범위 제한으로 _CHUNK_DAYS 단위로 분할 요청.
        """
        krx_code = yf_symbol.rsplit(".", 1)[0]
        isuCd    = _isin_from_krx_code(krx_code)

        # 날짜 범위를 청크로 분할 수집
        all_rows: list[dict] = []
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=self._CHUNK_DAYS - 1), end)
            chunk_rows = self.fetch_raw(krx_code, chunk_start, chunk_end, isuCd)
            all_rows.extend(chunk_rows)
            chunk_start = chunk_end + timedelta(days=1)
            if rate_delay > 0 and chunk_start <= end:
                _time.sleep(jittered_delay(rate_delay))

        if not all_rows:
            return []

        records: list[FlowRecord] = []
        f_streak = prev_f_streak
        i_streak = prev_i_streak
        p_streak = prev_p_streak

        for row in sorted(all_rows, key=lambda r: r.get("TRD_DD", "")):
            raw_date = (row.get("TRD_DD") or row.get("TDD_STR_DD") or row.get("BAS_DD") or "")
            raw_date = raw_date.replace("/", "").replace("-", "").strip()
            if len(raw_date) != 8 or not raw_date.isdigit():
                continue
            try:
                d = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
            except ValueError:
                continue
            if not (start <= d <= end):
                continue

            f_net = _parse_int(row.get(self._FOREIGN_COL, ""))
            i_net = _parse_int(row.get(self._INST_COL, ""))
            p_net = _parse_int(row.get(self._PERSONAL_COL, ""))
            f_streak = _next_streak(f_streak, f_net)
            i_streak = _next_streak(i_streak, i_net)
            p_streak = _next_streak(p_streak, p_net)

            records.append(FlowRecord(
                ticker=yf_symbol,
                trade_date=d,
                foreign_net=f_net,
                inst_net=i_net,
                foreign_streak=f_streak,
                inst_streak=i_streak,
                personal_net=p_net,
                personal_streak=p_streak,
            ))

        # 마지막 청크 이후 딜레이
        if rate_delay > 0:
            _time.sleep(jittered_delay(rate_delay))

        return records


def _parse_int(s: str) -> Optional[int]:
    """숫자 문자열 파싱. 빈값/'-'/NaN → None."""
    if not s:
        return None
    s = str(s).replace(",", "").strip()
    if s in ("-", "N/A", "nan", ""):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _make_krx_direct(
    krx_id: Optional[str],
    krx_pw: Optional[str],
    krx_session: Optional[str] = None,
    krx_visitor: Optional[str] = None,
) -> "_KrxDirectFetcher":
    """KrxDirectFetcher 초기화: warmup → (세션 주입 또는 login).

    krx_session 사용 시 warmup을 건너뜀: warmup 응답에서 서버가 새 익명
    JSESSIONID를 발급하면 주입한 인증 쿠키가 덮어씌워지기 때문.
    """
    fetcher = _KrxDirectFetcher()
    if krx_session:
        fetcher.inject_session(krx_session, krx_visitor)
    elif krx_id and krx_pw:
        fetcher.warmup()
        if not fetcher.login(krx_id, krx_pw) and getattr(fetcher, "last_error_code", None) in _TERMINAL_LOGIN_ERROR_CODES:
            # 재시도/세션갱신 대기로는 절대 안 풀리는 실패 — 여기서 바로 멈추지
            # 않으면 이후 전종목 fetch가 전부 빈 응답으로 실패하고, 그걸 "세션
            # 만료"로 오판해 30분 수동 갱신 대기 + Tor 회로 로테이션까지 거치며
            # 2시간 넘게 낭비한다(2026-09-07 실사고). 로그인 응답에서 이미
            # 원인이 확정됐으므로 즉시 중단.
            raise KrxTerminalAuthError(fetcher.last_error_code, fetcher.last_error_message)
    else:
        logger.warning(
            "[krx-direct] KRX_SESSION/KRX_ID/KRX_PW 미설정 — 인증 없이 시도합니다.\n"
            "  브라우저로 data.krx.co.kr 로그인 후 JSESSIONID 쿠키를 KRX_SESSION에 설정하세요."
        )
    return fetcher


# ── Kiwoom REST API 백엔드 ────────────────────────────────────
#
# ka10045 (종목별기관매매추이요청, POST /api/dostk/mrkcond)
# data.krx.co.kr 브라우저 세션 쿠키 없이 Bearer 토큰(KIWOOM_APPKEY/SECRETKEY)만으로
# 종목별 기관/외국인 일별 순매수를 날짜 범위로 조회. 거래일 캘린더(공휴일 포함)를
# 자체적으로 처리해 응답에 비거래일이 섞이지 않음 (실측: 2025-01 설 연휴 자동 제외).
#
# 응답 배열 키: stk_orgn_trde_trnsn (행당 거래일 1개)
#   dt                     : 거래일 YYYYMMDD
#   orgn_daly_nettrde_qty  : 기관 일별 순매수 수량 (부호 있는 정수 문자열, +/- 접두사 없음)
#   for_daly_nettrde_qty   : 외국인 일별 순매수 수량
#
# 개인(personal_net) 순매수는 이 TR에 제공되지 않음 — kiwoom 백엔드로 적재 시 None.


def _fetch_kiwoom_records(
    client: "KiwoomClient",
    yf_symbol: str,
    start: date,
    end: date,
    prev_f_streak: Optional[int],
    prev_i_streak: Optional[int],
) -> list[FlowRecord]:
    """ka10045로 단일 종목 기관/외국인 일별 순매수 조회 → FlowRecord 목록.

    cont-yn/next-key 페이지네이션 처리.
    """
    krx_code = yf_symbol.split(".")[0]
    all_rows: list[dict] = []
    cont_yn, next_key = "N", ""
    while True:
        data, hdrs = client._post(
            "/api/dostk/mrkcond", "ka10045",
            {
                "stk_cd":           krx_code,
                "strt_dt":          start.strftime("%Y%m%d"),
                "end_dt":           end.strftime("%Y%m%d"),
                "orgn_prsm_unp_tp": "0",
                "for_prsm_unp_tp":  "0",
            },
            cont_yn, next_key,
        )
        rows = data.get("stk_orgn_trde_trnsn") or []
        all_rows.extend(rows)
        resp_cont = hdrs.get("cont-yn", "N")
        resp_key  = hdrs.get("next-key", "")
        if resp_cont != "Y" or not resp_key:
            break
        cont_yn, next_key = "Y", resp_key

    if not all_rows:
        return []

    records: list[FlowRecord] = []
    f_streak = prev_f_streak
    i_streak = prev_i_streak

    for row in sorted(all_rows, key=lambda r: r.get("dt", "")):
        raw_date = (row.get("dt") or "").strip()
        if len(raw_date) != 8 or not raw_date.isdigit():
            continue
        try:
            d = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
        except ValueError:
            continue
        if not (start <= d <= end):
            continue

        i_net = _parse_int(row.get("orgn_daly_nettrde_qty", ""))
        f_net = _parse_int(row.get("for_daly_nettrde_qty", ""))
        f_streak = _next_streak(f_streak, f_net)
        i_streak = _next_streak(i_streak, i_net)

        records.append(FlowRecord(
            ticker=yf_symbol,
            trade_date=d,
            foreign_net=f_net,
            inst_net=i_net,
            foreign_streak=f_streak,
            inst_streak=i_streak,
        ))

    return records


async def run_kiwoom(
    pool,
    start: date,
    end: date,
    market: str,
    max_tickers: int,
    appkey: Optional[str],
    secretkey: Optional[str],
    force: bool = False,
) -> None:
    """kiwoom 백엔드: ka10045로 기관/외국인 일별 순매수 수집 (쿠키 불필요, 토큰 1회 발급)."""
    from analysis.chart_screener import get_all_tickers
    from core.db import get_prev_streak
    from data.kiwoom_aftermarket_sync import KiwoomClient

    if not appkey or not secretkey:
        logger.error(
            "[flow] [kiwoom] KIWOOM_APPKEY/KIWOOM_SECRETKEY 미설정 — .env에 추가하세요."
        )
        sys.exit(1)

    client = KiwoomClient(use_mock=False)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, client.issue_token, appkey, secretkey)

    tickers = _filter_tickers(get_all_tickers(), market, max_tickers)
    logger.info("[flow] [kiwoom] 대상 %d개 종목  %s ~ %s", len(tickers), start, end)

    total_saved = 0
    for i, (yf_sym, _name) in enumerate(tickers, 1):
        if not force:
            existing = await _get_existing_dates(pool, yf_sym, start, end)
            if _already_loaded(existing, start, end):
                logger.debug("[flow] %s 이미 저장됨, 건너뜀", yf_sym)
                continue

        prev_f, prev_i, _prev_p = await get_prev_streak(pool, yf_sym, start)

        records = await loop.run_in_executor(
            None,
            lambda sym=yf_sym, pf=prev_f, pi=prev_i: _fetch_kiwoom_records(
                client, sym, start, end, pf, pi
            ),
        )

        if not records:
            logger.debug("[flow] %s 데이터 없음", yf_sym)
        else:
            saved = await _save_batch(pool, records)
            total_saved += saved

        if i % 50 == 0 or i == len(tickers):
            logger.info("[flow] %d/%d 처리 완료  저장 누계: %d건", i, len(tickers), total_saved)

        await asyncio.sleep(0.2)  # Kiwoom REST API 호출 간격

    logger.info("[flow] 완료 — 총 저장: %d건", total_saved)


# ── pykrx 백엔드 ──────────────────────────────────────────────

def _pykrx_available() -> bool:
    try:
        import pykrx  # noqa: F401
        return True
    except ImportError:
        return False


def _fetch_pykrx(
    krx_code: str,
    start: date,
    end: date,
) -> Optional["pd.DataFrame"]:
    """pykrx로 단일 종목 투자자별 거래량(주) 조회.

    반환: index=날짜, columns=[..., 기관합계, 외국인합계, ...] DataFrame.
          실패 시 None.
    컬럼 위치 (pykrx 표준):
      idx 7 = 기관합계, idx 8 = 외국인합계
    """
    try:
        from pykrx import stock
        import pandas as pd

        fromdate = start.strftime("%Y%m%d")
        todate   = end.strftime("%Y%m%d")

        df = stock.get_market_trading_volume_by_investor(fromdate, todate, krx_code)

        if df is None or df.empty:
            return None

        return df

    except Exception as e:
        logger.debug("[pykrx] %s 조회 실패: %s", krx_code, e)
        return None


def _extract_nets(row: "pd.Series") -> tuple[Optional[int], Optional[int], Optional[int]]:
    """DataFrame 행에서 외국인/기관/개인 순매수(주) 추출.

    컬럼명이 한글로 있으면 직접 접근, KeyError 시 위치 기반 접근.
    pykrx 표준 컬럼 순서:
      [금융투자, 보험, 투신, 사모, 은행, 기타금융, 연기금, 기관합계, 외국인합계, 기타법인, 개인]
    """
    import pandas as pd

    def _get(col_name: str, fallback_pos: int) -> Optional[int]:
        try:
            v = row[col_name]
        except KeyError:
            cols = row.index.tolist()
            if len(cols) > fallback_pos:
                v = row.iloc[fallback_pos]
            else:
                return None
        if pd.isna(v):
            return None
        return int(v)

    inst     = _get("기관합계",   7)
    foreign  = _get("외국인합계", 8)
    personal = _get("개인",       10)
    return foreign, inst, personal


def fetch_pykrx_records(
    yf_symbol: str,
    start: date,
    end: date,
    prev_f_streak: Optional[int],
    prev_i_streak: Optional[int],
    prev_p_streak: Optional[int] = None,
) -> list[FlowRecord]:
    """단일 종목 수급 레코드 목록 반환 (스트릭 포함)."""
    import pandas as pd

    krx_code = yf_symbol.split(".")[0]  # "005930.KS" → "005930"
    df = _fetch_pykrx(krx_code, start, end)
    if df is None or df.empty:
        return []

    records: list[FlowRecord] = []
    f_streak = prev_f_streak
    i_streak = prev_i_streak
    p_streak = prev_p_streak

    for ts, row in df.iterrows():
        if isinstance(ts, str):
            try:
                d = date.fromisoformat(ts[:10])
            except ValueError:
                continue
        else:
            d = ts.date() if hasattr(ts, "date") else ts

        if not (start <= d <= end):
            continue

        foreign_net, inst_net, personal_net = _extract_nets(row)
        f_streak = _next_streak(f_streak, foreign_net)
        i_streak = _next_streak(i_streak, inst_net)
        p_streak = _next_streak(p_streak, personal_net)

        records.append(FlowRecord(
            ticker=yf_symbol,
            trade_date=d,
            foreign_net=foreign_net,
            inst_net=inst_net,
            foreign_streak=f_streak,
            inst_streak=i_streak,
            personal_net=personal_net,
            personal_streak=p_streak,
        ))

    return records


# ── CSV 백엔드 ────────────────────────────────────────────────

def _is_krx_style(header: list[str]) -> bool:
    """KRX 다운로드 CSV 형식 감지 (날짜, 종목코드, 종목명, ...)."""
    h = [c.strip() for c in header]
    return "종목코드" in h or "날짜" in h


def _code_to_symbol(code: str, market_hint: str = "") -> str:
    """KRX 6자리 코드 → yfinance 심볼 (시장 힌트 없으면 .KS 기본)."""
    code = code.strip().zfill(6)
    if ".KS" in market_hint or ".KQ" in market_hint:
        return market_hint.strip()
    if code.endswith(".KS") or code.endswith(".KQ"):
        return code
    # KOSDAQ: 첫자리 0~9, 구분 불가 → 입력에 시장 정보 없으면 KOSPI로 가정
    return f"{code}.KS"


def load_csv_records(csv_path: str) -> list[FlowRecord]:
    """CSV 파일에서 FlowRecord 목록 로드.

    지원 형식:
      1. 표준: date, ticker, foreign_net, inst_net
      2. KRX: 날짜, 종목코드, 종목명, 외국인순매수, 기관합계
    """
    records: list[FlowRecord] = []

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        krx_style = _is_krx_style(header)

        for i, row in enumerate(reader, start=2):
            try:
                if krx_style:
                    raw_date = row.get("날짜", "").strip()
                    code     = row.get("종목코드", "").strip()
                    f_raw    = row.get("외국인순매수", row.get("외국인합계", "0"))
                    i_raw    = row.get("기관합계", row.get("기관순매수", "0"))
                    ticker   = _code_to_symbol(code)
                else:
                    raw_date = row.get("date", "").strip()
                    ticker   = row.get("ticker", "").strip()
                    f_raw    = row.get("foreign_net", "0")
                    i_raw    = row.get("inst_net", "0")

                # 날짜 파싱 (YYYY-MM-DD 또는 YYYYMMDD)
                raw_date = raw_date.replace("-", "")
                if len(raw_date) != 8 or not raw_date.isdigit():
                    continue
                d = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))

                records.append(FlowRecord(
                    ticker=ticker,
                    trade_date=d,
                    foreign_net=_parse_int(f_raw),
                    inst_net=_parse_int(i_raw),
                ))
            except Exception as e:
                logger.warning("[csv] line %d 건너뜀: %s", i, e)

    logger.info("[csv] %s — %d행 로드", csv_path, len(records))
    return records


def _compute_streaks(records: list[FlowRecord]) -> list[FlowRecord]:
    """레코드 목록에서 종목별 스트릭 계산 (날짜 오름차순 전제)."""
    f_streaks: dict[str, Optional[int]] = defaultdict(lambda: None)
    i_streaks: dict[str, Optional[int]] = defaultdict(lambda: None)
    p_streaks: dict[str, Optional[int]] = defaultdict(lambda: None)

    records.sort(key=lambda r: (r.ticker, r.trade_date))
    for rec in records:
        rec.foreign_streak  = _next_streak(f_streaks[rec.ticker], rec.foreign_net)
        rec.inst_streak     = _next_streak(i_streaks[rec.ticker], rec.inst_net)
        rec.personal_streak = _next_streak(p_streaks[rec.ticker], rec.personal_net)
        f_streaks[rec.ticker] = rec.foreign_streak
        i_streaks[rec.ticker] = rec.inst_streak
        p_streaks[rec.ticker] = rec.personal_streak

    return records


# ── DB 저장 ───────────────────────────────────────────────────

async def _save_batch(pool, records: list[FlowRecord]) -> int:
    """FlowRecord 목록을 daily_flow에 upsert. 저장 건수 반환."""
    from core.db import save_daily_flow

    saved = 0
    for rec in records:
        try:
            await save_daily_flow(
                pool,
                ticker=rec.ticker,
                trade_date=rec.trade_date,
                foreign_net=rec.foreign_net,
                inst_net=rec.inst_net,
                foreign_streak=rec.foreign_streak,
                inst_streak=rec.inst_streak,
                personal_net=rec.personal_net,
                personal_streak=rec.personal_streak,
            )
            saved += 1
        except Exception as e:
            logger.debug("[db] %s %s 저장 실패: %s", rec.ticker, rec.trade_date, e)
    return saved


async def _get_existing_dates(pool, ticker: str, start: date, end: date) -> set[date]:
    """daily_flow에서 이미 저장된 날짜 집합 반환 (증분 스킵용)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT trade_date FROM daily_flow
                WHERE ticker = $1 AND trade_date BETWEEN $2 AND $3
                """,
                ticker, start, end,
            )
        return {r["trade_date"] for r in rows}
    except Exception:
        return set()


# ── 메인 워크플로우 ───────────────────────────────────────────

def _filter_tickers(all_tickers, market: str, max_tickers: int) -> list[tuple[str, str]]:
    if market == "KOSPI":
        tickers = [(t, n) for t, n, _ in all_tickers if t.endswith(".KS")]
    elif market == "KOSDAQ":
        tickers = [(t, n) for t, n, _ in all_tickers if t.endswith(".KQ")]
    else:
        tickers = [(t, n) for t, n, _ in all_tickers]
    if max_tickers > 0:
        tickers = tickers[:max_tickers]
    return tickers


def _already_loaded(existing: set, start: date, end: date) -> bool:
    # 달력일 기준이 아닌 거래일 기준으로 비교 (KRX 연간 거래일 ≈ 250일).
    # 달력일(~365) × 0.9 = 328 > 실제 저장 가능한 거래일(~250) 이므로
    # 달력일 비교 시 증분 스킵이 절대 동작하지 않음.
    expected = int((end - start).days * 250 / 365)
    return len(existing) >= max(1, expected * 0.9)


# 연속 빈 응답 N건 → 세션 만료 의심 → Samsung 프로브로 확인
_SESSION_EXPIRY_THRESHOLD = 5

# KRX_SESSION 수동 갱신 대기 최대 시간(분) — daily_flow_sync_job이 사람의
# 응답을 기다리며 무한정 멈춰 다음 스케줄 실행을 막는 것을 방지.
_SESSION_WAIT_TIMEOUT_MIN = 30


async def _handle_possible_expiry(
    fetcher: "_KrxDirectFetcher",
    consecutive_empty: int,
    krx_id: Optional[str] = None,
    krx_pw: Optional[str] = None,
) -> bool:
    """연속 빈 응답 임계값 도달 시 세션 만료 여부 확인 후 갱신 대기.

    Samsung(005930) 단기 프로브로 확인:
      - 데이터 반환 → 세션 정상 (해당 종목에 데이터 없는 것) → False 반환
      - 빈 응답 → 세션 만료 → .env 갱신 대기 후 True 반환 (카운터 리셋)

    갱신 대기가 타임아웃되면 fetcher에 _session_wait_exhausted 플래그를 세워
    이후 남은 종목들에 대해서는 프로브/대기 없이 즉시 False를 반환한다 —
    그렇지 않으면 남은 종목마다 다시 _SESSION_WAIT_TIMEOUT_MIN분씩 대기하며
    daily_flow_sync_job(max_instances=1)을 사실상 영구히 묶어버리게 된다.
    """
    if getattr(fetcher, "_session_wait_exhausted", False):
        return False
    if consecutive_empty < _SESSION_EXPIRY_THRESHOLD:
        return False

    loop = asyncio.get_running_loop()
    yesterday = date.today() - timedelta(days=1)
    isuCd = _isin_from_krx_code("005930")
    probe_rows = await loop.run_in_executor(
        None, lambda: fetcher.fetch_raw("005930", yesterday - timedelta(days=7), yesterday, isuCd)
    )
    if probe_rows:
        return False  # 세션 정상

    # 세션이 실제로 만료된 게 아니라 현재 Tor 출구 노드가 도중에 새로
    # 차단됐을 가능성 — 이미 실패한 상태라 잃을 게 없으므로 회로만 바꿔
    # 한 번 더 확인 (쿠키는 그대로 유지, IP만 변경).
    # 주의: KRX_SESSION 쿠키가 발급 IP에 묶여 있을 가능성이 있어 이 시도가
    # 항상 성공하진 않는다 — 실패하면 그대로 아래 재로그인/수동 갱신 대기로 넘어간다.
    if new_identity():
        logger.info("[flow] Tor 회로 로테이션 후 세션 재확인...")
        await asyncio.sleep(3)
        probe_rows2 = await loop.run_in_executor(
            None, lambda: fetcher.fetch_raw("005930", yesterday - timedelta(days=7), yesterday, isuCd)
        )
        if probe_rows2:
            logger.info("[flow] 회로 로테이션으로 복구 — 수집을 재개합니다.")
            return True

    # KRX_ID/KRX_PW가 있으면 사람이 브라우저에서 쿠키를 복사해줄 때까지
    # 기다리기 전에 자동 재로그인을 먼저 시도한다. 주의: 기존 세션이
    # 살아있는 상태에서 재로그인을 시도하면 KRX가 CD011("중복 로그인")로
    # 거부하며 기존 세션까지 끊어버릴 수 있다 — 이미 연속 5회 빈 응답 +
    # 회로 로테이션도 실패한 뒤이므로 기존 세션은 사실상 죽은 것으로 보고
    # 감수한다.
    if krx_id and krx_pw:
        logger.info("[flow] KRX_ID/KRX_PW로 자동 재로그인 시도...")
        login_ok = await loop.run_in_executor(None, lambda: fetcher.login(krx_id, krx_pw))
        if login_ok:
            probe_rows3 = await loop.run_in_executor(
                None, lambda: fetcher.fetch_raw("005930", yesterday - timedelta(days=7), yesterday, isuCd)
            )
            if probe_rows3:
                logger.info("[flow] 자동 재로그인으로 복구 — 수집을 재개합니다.")
                return True
        elif getattr(fetcher, "last_error_code", None) in _TERMINAL_LOGIN_ERROR_CODES:
            # 재로그인 자체가 계정 문제로 거부됨 — 세션 쿠키가 아니라 계정이
            # 막힌 것이므로, 아래 "KRX_SESSION 수동 갱신 대기 30분"은 의미가
            # 없다(애초에 KRX_SESSION을 안 쓰는 KRX_ID/KRX_PW 구성일 수도 있음).
            # 여기서 즉시 중단해 나머지 종목들이 같은 실패를 반복하며
            # Tor 회로 로테이션으로 시간을 더 쓰는 것을 막는다.
            fetcher._session_wait_exhausted = True
            raise KrxTerminalAuthError(fetcher.last_error_code, fetcher.last_error_message)

    logger.warning(
        "[flow] 세션 만료 감지 (연속 빈 응답 %d건) — "
        ".env의 KRX_SESSION을 브라우저에서 새로 복사해 저장하세요. 30초마다 재확인합니다 "
        "(최대 %d분, 응답 없으면 잡을 종료합니다).",
        consecutive_empty, _SESSION_WAIT_TIMEOUT_MIN,
    )
    current_session = os.environ.get("KRX_SESSION", "")
    max_attempts = _SESSION_WAIT_TIMEOUT_MIN * 60 // 30
    for attempt in range(1, max_attempts + 1):
        await asyncio.sleep(30)
        refresh_env()  # 사람이 갱신한 .env의 KRX_SESSION을 다시 읽음
        new_session = os.environ.get("KRX_SESSION", "")
        if new_session and new_session != current_session:
            fetcher.inject_session(new_session, os.environ.get("KRX_VISITOR"))
            logger.info("[flow] KRX_SESSION 갱신 완료 — 수집을 재개합니다.")
            return True
        logger.info("[flow] KRX_SESSION 미갱신 (%d/%d) — 30초 후 재확인...", attempt, max_attempts)

    # 타임아웃 — daily_flow_sync_job(max_instances=1)이 영구히 막히는 것을
    # 방지하기 위해 여기서 포기한다. 플래그를 세워 남은 종목들이 다시
    # 이 대기 루프에 들어가지 않도록 한다 (안 그러면 종목마다 타임아웃을
    # 반복하며 잡이 사실상 영구히 안 끝남). 남은 종목은 "데이터 없음"으로
    # 건너뛰어지고, 다음날 --incremental이 재시도한다.
    fetcher._session_wait_exhausted = True
    logger.error(
        "[flow] KRX_SESSION 미갱신 상태로 %d분 경과 — 세션 갱신 대기를 포기하고 "
        "남은 수집을 건너뜁니다. 다음 스케줄 실행을 막지 않기 위함.",
        _SESSION_WAIT_TIMEOUT_MIN,
    )
    return False


async def run_pykrx(
    pool,
    start: date,
    end: date,
    market: str,
    max_tickers: int,
    force: bool = False,
) -> None:
    """pykrx 백엔드: 전 종목 순차 수집 후 저장."""
    from analysis.chart_screener import get_all_tickers
    from core.db import get_prev_streak

    tickers = _filter_tickers(get_all_tickers(), market, max_tickers)
    logger.info("[flow] 대상 %d개 종목  %s ~ %s", len(tickers), start, end)

    total_saved = 0
    for i, (yf_sym, name) in enumerate(tickers, 1):
        # 이미 저장된 날짜 확인 (증분, --force 시 건너뜀)
        if not force:
            existing = await _get_existing_dates(pool, yf_sym, start, end)
            if _already_loaded(existing, start, end):
                logger.debug("[flow] %s 이미 저장됨, 건너뜀", yf_sym)
                continue

        # 전일 스트릭 로드
        prev_f, prev_i, prev_p = await get_prev_streak(pool, yf_sym, start)

        # pykrx 수집 (동기 → executor)
        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            None, fetch_pykrx_records, yf_sym, start, end, prev_f, prev_i, prev_p
        )

        if not records:
            logger.debug("[flow] %s 데이터 없음", yf_sym)
            continue

        saved = await _save_batch(pool, records)
        total_saved += saved

        if i % 50 == 0 or i == len(tickers):
            logger.info("[flow] %d/%d 처리 완료  저장 누계: %d건", i, len(tickers), total_saved)

    logger.info("[flow] 완료 — 총 저장: %d건", total_saved)


async def run_krx_direct(
    pool,
    start: date,
    end: date,
    market: str,
    max_tickers: int,
    krx_id: Optional[str],
    krx_pw: Optional[str],
    krx_session: Optional[str] = None,
    krx_visitor: Optional[str] = None,
    force: bool = False,
) -> int:
    """krx-direct 백엔드: pykrx 없이 data.krx.co.kr 직접 호출.

    반환값(total_saved)은 호출부(main())가 --incremental 실행에서 0건이면
    비정상 종료(exit 1)로 판단해 daily_flow_sync_job의 텔레그램 알림이
    실제로 발동하도록 하는 데 쓰인다 — Tor Browser가 꺼져있어 모든 요청이
    연결 자체가 안 되는 경우 fetch_raw는 예외 없이 조용히 []를 반환하므로
    (403이 아니라 연결 오류라 로테이션도 안 됨) exit code만으로는 이 실패를
    감지할 수 없었다.
    """
    from analysis.chart_screener import get_all_tickers
    from core.db import get_prev_streak

    fetcher = _make_krx_direct(krx_id, krx_pw, krx_session, krx_visitor)

    tickers = _filter_tickers(get_all_tickers(), market, max_tickers)
    # KRX가 세션당 요청 횟수 기준으로 차단하는 것으로 추정 — 알파벳/등록순 그대로
    # 처리하면 매일 정확히 같은 ~945번째 지점에서 죽어 그 뒤 종목은 영구히
    # 누락된다(2026-07-14 투자 확인: 07-10/07-13 모두 동일 인덱스에서 붕괴).
    # 순서를 매일 셔플해서 하루에 다 못 채우더라도 여러 날에 걸쳐 전체
    # 종목이 골고루 커버되도록 한다.
    random.shuffle(tickers)
    logger.info("[flow] [krx-direct] 대상 %d개 종목  %s ~ %s", len(tickers), start, end)

    total_saved = 0
    consecutive_empty = 0
    for i, (yf_sym, _name) in enumerate(tickers, 1):
        if not force:
            existing = await _get_existing_dates(pool, yf_sym, start, end)
            if _already_loaded(existing, start, end):
                logger.debug("[flow] %s 이미 저장됨, 건너뜀", yf_sym)
                continue

        prev_f, prev_i, prev_p = await get_prev_streak(pool, yf_sym, start)

        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            None,
            lambda sym=yf_sym, pf=prev_f, pi=prev_i, pp=prev_p: fetcher.fetch_records(
                sym, start, end, pf, pi, pp
            ),
        )

        if not records:
            consecutive_empty += 1
            if await _handle_possible_expiry(fetcher, consecutive_empty, krx_id, krx_pw):
                consecutive_empty = 0
            logger.debug("[flow] %s 데이터 없음", yf_sym)
            continue

        consecutive_empty = 0
        saved = await _save_batch(pool, records)
        total_saved += saved

        if i % 50 == 0 or i == len(tickers):
            logger.info("[flow] %d/%d 처리 완료  저장 누계: %d건", i, len(tickers), total_saved)

    logger.info("[flow] 완료 — 총 저장: %d건", total_saved)
    return total_saved


async def run_csv(pool, csv_path: str) -> None:
    """CSV 백엔드: 파일 로드 → 스트릭 계산 → 저장."""
    records = load_csv_records(csv_path)
    if not records:
        logger.warning("[csv] 저장할 레코드 없음")
        return

    records = _compute_streaks(records)

    saved = await _save_batch(pool, records)
    logger.info("[csv] 완료 — 저장: %d/%d건", saved, len(records))


def probe_raw_response(
    krx_code: str,
    krx_id: Optional[str],
    krx_pw: Optional[str],
    krx_session: Optional[str] = None,
    krx_visitor: Optional[str] = None,
) -> None:
    """--probe: 단일 종목 응답 구조를 JSON으로 출력 (필드명 확인용)."""
    fetcher = _make_krx_direct(krx_id, krx_pw, krx_session, krx_visitor)
    yesterday = date.today() - timedelta(days=1)
    start = yesterday - timedelta(days=30)
    krx_code_6 = krx_code.split(".")[0]
    isuCd = _isin_from_krx_code(krx_code_6)
    rows = fetcher.fetch_raw(krx_code_6, start, yesterday, isuCd)
    if not rows:
        print(f"[probe] 응답 없음 — 인증 실패 또는 네트워크 차단")
        print("  확인: KRX_SESSION(브라우저 JSESSIONID) 또는 KRX_ID/KRX_PW 설정, 한국 ISP 또는 VPN 사용 여부")
        return
    print(f"[probe] {len(rows)}행 수신 (ISIN={isuCd}). 첫 3행:")
    for row in rows[:3]:
        print(_json.dumps(row, ensure_ascii=False, indent=2))
    print(f"\n[probe] 컬럼: {list(rows[0].keys())}")


def _probe_login(krx_id: Optional[str], krx_pw: Optional[str]) -> None:
    """--probe-login: 로그인 응답 원문 출력 (KRX_ID/KRX_PW 진단용)."""
    import json as _json_diag
    if not krx_id or not krx_pw:
        print("[probe-login] KRX_ID 또는 KRX_PW가 .env에 설정되지 않았습니다.")
        return

    fetcher = _KrxDirectFetcher()
    print(f"[probe-login] warmup ...")
    fetcher.warmup()

    print(f"[probe-login] 로그인 시도 (mbrId={krx_id[:2]}***) ...")
    try:
        raw = fetcher._post(fetcher._LOGIN, {
            "mbrNm": "", "telNo": "", "di": "", "certType": "",
            "mbrId": krx_id, "pw": krx_pw,
        })
        text = fetcher._decode(raw)
        print(f"[probe-login] 응답 ({len(raw)}bytes):\n{text[:500]}")
        try:
            parsed = _json_diag.loads(text)
            print(f"[probe-login] _error_code   : {parsed.get('_error_code')}")
            print(f"[probe-login] _error_message: {parsed.get('_error_message')}")
        except Exception:
            pass
    except Exception as e:
        print(f"[probe-login] 요청 실패: {e}")


# ── CLI ───────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="외국인/기관 순매수 이력을 daily_flow 테이블에 적재"
    )
    parser.add_argument("--start",   default=None, help="시작일 YYYY-MM-DD (기본: 2년 전)")
    parser.add_argument("--end",     default=None, help="종료일 YYYY-MM-DD (기본: 어제)")
    parser.add_argument("--market",  default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--max",     type=int, default=0,
                        help="최대 티커 수 (기본 0=전종목, 테스트용)")
    parser.add_argument("--incremental", action="store_true",
                        help="어제 데이터만 적재 (스케줄러용)")
    parser.add_argument("--backend", default="krx-direct",
                        choices=["krx-direct", "pykrx", "csv", "kiwoom"],
                        help="수집 백엔드 (기본: krx-direct)")
    parser.add_argument("--csv",     default=None, metavar="FILE",
                        help="CSV 파일 임포트 경로 (--backend csv 시 필수)")
    parser.add_argument("--probe",       default=None, metavar="CODE",
                        help="응답 구조 확인용 단일 종목 테스트 (예: --probe 005930)")
    parser.add_argument("--probe-login", action="store_true",
                        help="로그인 응답 원문 출력 (KRX_ID/KRX_PW 진단용, DB 불필요)")
    parser.add_argument("--force", action="store_true",
                        help="이미 저장된 날짜도 덮어쓰기 (personal_net 백필용)")
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()

    krx_id = os.environ.get("KRX_ID")
    krx_pw = os.environ.get("KRX_PW")
    krx_session = os.environ.get("KRX_SESSION")
    krx_visitor = os.environ.get("KRX_VISITOR")

    # --probe-login: DB 없이 로그인 응답 원문 출력
    if args.probe_login:
        _probe_login(krx_id, krx_pw)
        return

    # --probe: DB 없이 응답 구조만 확인
    if args.probe:
        probe_raw_response(args.probe, krx_id, krx_pw, krx_session, krx_visitor)
        return

    yesterday = date.today() - timedelta(days=1)

    if args.incremental:
        start = end = last_trading_day(date.today())
    else:
        end   = date.fromisoformat(args.end)   if args.end   else yesterday
        start = date.fromisoformat(args.start) if args.start else end - timedelta(days=730)

    if start > end:
        logger.error("start(%s) > end(%s)", start, end)
        sys.exit(1)

    # DB 풀 생성
    import core.db as _db
    pool = await _db.create_pool()
    await _db.init_db(pool)

    logger.info("[flow] 시작  backend=%s  %s ~ %s  %s  최대티커: %s",
                args.backend, start, end, args.market, args.max or "전종목")

    try:
        if args.backend == "csv":
            if not args.csv or not os.path.isfile(args.csv):
                logger.error("--backend csv 사용 시 --csv <파일경로> 필수")
                sys.exit(1)
            await run_csv(pool, args.csv)

        elif args.backend == "pykrx":
            if not _pykrx_available():
                logger.error("pykrx 미설치. pip install pykrx 후 재실행.")
                sys.exit(1)
            if not krx_id or not krx_pw:
                logger.warning(
                    "KRX_ID/KRX_PW 미설정 — 인증 없이 시도합니다. 실패 시 .env 확인."
                )
            await run_pykrx(pool, start, end, args.market, args.max, force=args.force)

        elif args.backend == "kiwoom":
            await run_kiwoom(
                pool, start, end, args.market, args.max,
                os.environ.get("KIWOOM_APPKEY"), os.environ.get("KIWOOM_SECRETKEY"),
                force=args.force,
            )

        else:  # krx-direct (기본)
            try:
                total_saved = await run_krx_direct(
                    pool, start, end, args.market, args.max, krx_id, krx_pw, krx_session, krx_visitor,
                    force=args.force,
                )
            except KrxTerminalAuthError as e:
                # 세션 만료가 아니라 계정 자체가 막힌 상태(CD010/CD007 등, 원인은
                # 실행마다 다를 수 있음) — 재시도해도 안 풀리므로 30분 세션 대기나
                # Tor 회로 로테이션을 거치지 않고 여기서 바로 중단한다. "계정 자체가
                # 막힌 상태"라는 문구는 infra_jobs.py의 daily_flow_sync_job이 텔레그램
                # 알림 문구를 고르는 데 그대로 매칭하므로 표현을 바꾸지 말 것 — 실제
                # KRX 원인(err_code/err_msg)은 알림에 그대로 실어 보낸다.
                logger.error(
                    "[flow] KRX 로그인 실패 [%s] — 계정 자체가 막힌 상태(재시도/세션갱신 "
                    "불가), data.krx.co.kr에서 직접 확인 필요 (KRX_SESSION 문제 아님): %s",
                    e.err_code, e.err_msg,
                )
                sys.exit(1)
            # --incremental은 항상 "어제"라는 아직 적재 안 된 새 날짜를 대상으로
            # 하므로 정상 동작이면 0건일 수 없다. 0건이면 네트워크/인증이 전부
            # 조용히 실패한 것(예: Tor Browser가 꺼져있어 모든 요청이 연결 자체가
            # 안 되는 경우 — fetch_raw가 예외 없이 []를 반환해 exit code로는
            # 감지가 안 됨) — daily_flow_sync_job의 텔레그램 알림이 실제로
            # 발동하도록 여기서 비정상 종료시킨다.
            if args.incremental and total_saved == 0:
                logger.error("[flow] --incremental 실행에서 0건 수집 — 네트워크/인증 실패로 판단, exit(1)")
                sys.exit(1)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
