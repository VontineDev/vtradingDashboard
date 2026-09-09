"""
Unit tests for krx_flow_sync.py — _KrxDirectFetcher (login, warmup, fetch_raw,
fetch_records), pure helpers (_parse_int, _next_streak), CSV import, and streak calc.

No network calls: requests.Session is bypassed; _post is mocked at the method level.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.dates import last_trading_day as _last_trading_day
from core.tor import jittered_delay as _jittered_delay, new_identity as _tor_new_identity
from data.krx_flow_sync import (
    FlowRecord,
    KrxTerminalAuthError,
    _KrxDirectFetcher,
    _compute_streaks,
    _fetch_kiwoom_records,
    _handle_possible_expiry,
    _make_krx_direct,
    _next_streak,
    _parse_int,
    load_csv_records,
    run_krx_direct,
)


# ── fixture helper ────────────────────────────────────────────────────────────

def _bare_fetcher() -> _KrxDirectFetcher:
    """Create _KrxDirectFetcher bypassing the `import requests` in __init__."""
    fetcher = object.__new__(_KrxDirectFetcher)
    fetcher._session = MagicMock()
    fetcher._authenticated = False
    fetcher._logout_warned = False
    fetcher.last_error_code = None
    fetcher.last_error_message = ""
    return fetcher


# ── _parse_int ────────────────────────────────────────────────────────────────

class TestParseInt:
    def test_plain_number(self):
        assert _parse_int("12345") == 12345

    def test_comma_separated(self):
        assert _parse_int("1,234,567") == 1234567

    def test_negative(self):
        assert _parse_int("-5000") == -5000

    def test_float_string_truncates(self):
        assert _parse_int("123.0") == 123

    def test_empty_returns_none(self):
        assert _parse_int("") is None

    def test_dash_returns_none(self):
        assert _parse_int("-") is None

    def test_na_returns_none(self):
        assert _parse_int("N/A") is None

    def test_nan_returns_none(self):
        assert _parse_int("nan") is None

    def test_zero(self):
        assert _parse_int("0") == 0


# ── _next_streak ──────────────────────────────────────────────────────────────

class TestNextStreak:
    def test_net_none_returns_zero(self):
        assert _next_streak(5, None) == 0

    def test_positive_from_zero(self):
        assert _next_streak(0, 100) == 1

    def test_positive_from_none(self):
        assert _next_streak(None, 100) == 1

    def test_positive_increments(self):
        assert _next_streak(3, 100) == 4

    def test_negative_from_zero(self):
        assert _next_streak(0, -100) == -1

    def test_negative_decrements(self):
        assert _next_streak(-2, -500) == -3

    def test_zero_net_resets(self):
        assert _next_streak(5, 0) == 0

    def test_buy_to_sell_reversal(self):
        assert _next_streak(3, -100) == -1

    def test_sell_to_buy_reversal(self):
        assert _next_streak(-3, 100) == 1


# ── warmup() ─────────────────────────────────────────────────────────────────

class TestWarmup:
    def test_returns_true_on_success(self):
        fetcher = _bare_fetcher()
        cast(MagicMock, fetcher._session).get.return_value = MagicMock(status_code=200)
        assert fetcher.warmup() is True

    def test_returns_false_on_connection_error(self):
        fetcher = _bare_fetcher()
        cast(MagicMock, fetcher._session).get.side_effect = ConnectionError("timeout")
        assert fetcher.warmup() is False

    def test_does_not_raise(self):
        fetcher = _bare_fetcher()
        cast(MagicMock, fetcher._session).get.side_effect = RuntimeError("unexpected")
        assert fetcher.warmup() is False  # swallowed, not raised


# ── login() ───────────────────────────────────────────────────────────────────

class TestLoginMethod:

    def _run_login(self, response_bytes: bytes) -> tuple[bool, _KrxDirectFetcher]:
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", return_value=response_bytes):
            result = fetcher.login("user", "pw")
        return result, fetcher

    # --- success paths -------------------------------------------------------

    def test_success_on_json_with_success_key(self):
        result, fetcher = self._run_login(b'{"result":"success","redirect":"/home"}')
        assert result is True
        assert fetcher._authenticated is True

    def test_success_case_insensitive(self):
        result, _ = self._run_login(b"SUCCESS")
        assert result is True

    def test_success_on_exact_ok_response(self):
        result, fetcher = self._run_login(b"OK")
        assert result is True
        assert fetcher._authenticated is True

    def test_success_on_ok_with_whitespace(self):
        result, _ = self._run_login(b"  ok  ")
        assert result is True

    def test_success_on_cd001_normal_response(self):
        """Regression (/plan-eng-review live verification, 2026-07-11):
        real KRX success response is {"_error_code":"CD001","_error_message":
        "정상"} — contains neither "success" nor "OK", so it was being
        misjudged as a failed login until this fix."""
        cd001 = '{"previousMemberYn":false,"MDC_MBR_TP_CD":"P","MBR_NO":"2000095104","_error_code":"CD001","_error_message":"정상"}'.encode("utf-8")
        result, fetcher = self._run_login(cd001)
        assert result is True
        assert fetcher._authenticated is True

    # --- failure paths -------------------------------------------------------

    def test_failure_on_logout_response(self):
        result, fetcher = self._run_login(b"LOGOUT")
        assert result is False
        assert fetcher._authenticated is False

    def test_failure_on_empty_response(self):
        result, fetcher = self._run_login(b"")
        assert result is False
        assert fetcher._authenticated is False

    def test_failure_on_unrecognized_response(self):
        result, fetcher = self._run_login(b"ERROR: invalid credentials")
        assert result is False
        assert fetcher._authenticated is False

    def test_failure_does_not_set_authenticated(self):
        _, fetcher = self._run_login(b"LOGOUT")
        assert fetcher._authenticated is False

    def test_failure_on_network_exception(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", side_effect=ConnectionError("timeout")):
            result = fetcher.login("user", "pw")
        assert result is False
        assert fetcher._authenticated is False

    def test_not_ok_substring_rejected(self):
        """'Not OK, retry' contains 'OK' — must NOT be treated as success."""
        result, fetcher = self._run_login(b"Not OK, credentials invalid")
        assert result is False, (
            "login() must use exact 'OK' match, not substring — "
            "'Not OK' should be rejected"
        )
        assert fetcher._authenticated is False

    def test_login_does_not_raise_on_http_error(self):
        """Any exception from _post is caught and returns False."""
        import requests
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", side_effect=requests.HTTPError("401")):
            result = fetcher.login("user", "pw")
        assert result is False

    # --- encoding fixes -------------------------------------------------------

    def test_utf8_error_response_decoded_correctly(self):
        """UTF-8 JSON error response must NOT be decoded as EUC-KR (mojibake)."""
        # KRX login endpoint returns UTF-8 JSON, not EUC-KR.
        # Before fix: raw.decode("euc-kr") → garbled Korean.
        # After fix: _decode() tries UTF-8 first → clean message.
        response = '{"_error_code":"CD004","_error_message":"아이디 또는 비밀번호가 잘못되었습니다"}'.encode("utf-8")
        result, _ = self._run_login(response)
        assert result is False  # error response correctly rejected

    def test_cd003_returns_false_with_actionable_message(self):
        """CD003 = KRX generic auth failure → False, no retry."""
        cd003 = b'{"_error_code":"CD003","_error_message":"service error"}'
        result, fetcher = self._run_login(cd003)
        assert result is False
        assert fetcher._authenticated is False

    def test_cd003_single_post_call_no_retry(self):
        """CD003 must NOT trigger a second _post call (retry logic removed)."""
        cd003 = b'{"_error_code":"CD003"}'
        fetcher = _bare_fetcher()
        call_count = [0]
        def capture(url, data, **kwargs):
            call_count[0] += 1
            return cd003
        with patch.object(fetcher, "_post", side_effect=capture):
            fetcher.login("user", "pw")
        assert call_count[0] == 1  # only one attempt, no retry

    # --- last_error_code tracking (2026-09-07 CD010 incident) ----------------

    def test_cd010_records_terminal_error_code(self):
        """CD010 ("패스워드 변경 필요") must be recorded on the fetcher so
        callers (_make_krx_direct/_handle_possible_expiry) can fail fast
        instead of treating it like a recoverable session expiry."""
        cd010 = '{"_error_code":"CD010","_error_message":"패스워드 변경 필요"}'.encode("utf-8")
        result, fetcher = self._run_login(cd010)
        assert result is False
        assert fetcher.last_error_code == "CD010"
        assert fetcher.last_error_message == "패스워드 변경 필요"

    def test_cd003_records_error_code_too(self):
        """Non-terminal failures also record their code — only the caller
        decides which codes are terminal, not login() itself."""
        cd003 = b'{"_error_code":"CD003","_error_message":"service error"}'
        _, fetcher = self._run_login(cd003)
        assert fetcher.last_error_code == "CD003"

    def test_success_clears_previously_recorded_error_code(self):
        """A prior failed attempt's error code must not linger after a
        subsequent successful login (e.g. auto-relogin recovering)."""
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", return_value=b'{"_error_code":"CD010","_error_message":"x"}'):
            fetcher.login("user", "pw")
        assert fetcher.last_error_code == "CD010"
        cd001 = '{"_error_code":"CD001","_error_message":"정상"}'.encode("utf-8")
        with patch.object(fetcher, "_post", return_value=cd001):
            result = fetcher.login("user", "pw")
        assert result is True
        assert fetcher.last_error_code is None


# ── _decode helper ────────────────────────────────────────────────────────────

class TestDecodeHelper:
    def test_utf8_bytes_decoded_as_utf8(self):
        fetcher = _bare_fetcher()
        korean = "외국인합계"
        assert fetcher._decode(korean.encode("utf-8")) == korean

    def test_euc_kr_bytes_fall_back_to_euc_kr(self):
        fetcher = _bare_fetcher()
        korean = "외국인합계"
        assert fetcher._decode(korean.encode("euc-kr")) == korean

    def test_ascii_bytes_decoded_as_utf8(self):
        fetcher = _bare_fetcher()
        assert fetcher._decode(b"LOGOUT") == "LOGOUT"


# ── fetch_raw() ───────────────────────────────────────────────────────────────

class TestFetchRaw:
    def _json_bytes(self, rows: list, key: str = "output") -> bytes:
        return json.dumps({key: rows}).encode("utf-8")

    def test_returns_rows_from_output_key(self):
        fetcher = _bare_fetcher()
        rows = [{"INVST_TP_NM": "외국인합계", "TDD_STR_DD": "20250103", "NETBID_TRDVOL": "1000"}]
        with patch.object(fetcher, "_post", return_value=self._json_bytes(rows, "output")):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
        assert result == rows

    def test_returns_rows_from_outblock1_key(self):
        fetcher = _bare_fetcher()
        rows = [{"INVST_TP_NM": "기관합계", "TDD_STR_DD": "20250103", "NETBID_TRDVOL": "-500"}]
        with patch.object(fetcher, "_post", return_value=self._json_bytes(rows, "OutBlock_1")):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
        assert result == rows

    def test_returns_empty_on_logout(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", return_value=b"LOGOUT"):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
        assert result == []

    def test_returns_empty_on_empty_bytes(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", return_value=b""):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
        assert result == []

    def test_returns_empty_on_request_exception(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", side_effect=ConnectionError("down")):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
        assert result == []

    def test_returns_empty_on_invalid_json(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", return_value=b"not json"):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
        assert result == []

    def test_utf8_json_response_parsed_correctly(self):
        """UTF-8 JSON (real KRX format) must be parsed, not garbled.

        Before fix: raw.decode("euc-kr") garbled 외국인합계/기관합계 so
        investor type names never matched → fetch_records() returned 0 records.
        After fix: _decode() returns clean UTF-8 text.
        """
        rows = [
            {"INVST_TP_NM": "외국인합계", "TDD_STR_DD": "20250103", "NETBID_TRDVOL": "1000"},
            {"INVST_TP_NM": "기관합계",   "TDD_STR_DD": "20250103", "NETBID_TRDVOL": "-500"},
        ]
        # Encode as UTF-8 (what KRX actually sends)
        utf8_response = json.dumps({"output": rows}).encode("utf-8")
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", return_value=utf8_response):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
        # Investor type names must survive the decode intact
        assert len(result) == 2
        assert result[0]["INVST_TP_NM"] == "외국인합계"
        assert result[1]["INVST_TP_NM"] == "기관합계"


# ── fetch_records() ───────────────────────────────────────────────────────────

class TestFetchRecords:
    def _rows(self, date_str: str, foreign: str, inst: str) -> list[dict]:
        # 신API 포맷: 날짜당 1행, TRDVAL1=외국인합계, TRDVAL2=기관합계
        return [{"TRD_DD": date_str, "TRDVAL1": foreign, "TRDVAL2": inst}]

    def test_parses_foreign_and_inst(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "fetch_raw", return_value=self._rows("20250103", "1000", "-500")):
            records = fetcher.fetch_records(
                "005930.KS", date(2025, 1, 1), date(2025, 1, 5),
                prev_f_streak=None, prev_i_streak=None, rate_delay=0,
            )
        assert len(records) == 1
        rec = records[0]
        assert rec.ticker == "005930.KS"
        assert rec.trade_date == date(2025, 1, 3)
        assert rec.foreign_net == 1000
        assert rec.inst_net == -500

    def test_streak_accumulates_across_days(self):
        fetcher = _bare_fetcher()
        rows = self._rows("20250103", "1000", "200") + self._rows("20250106", "500", "300")
        with patch.object(fetcher, "fetch_raw", return_value=rows):
            records = fetcher.fetch_records(
                "005930.KS", date(2025, 1, 1), date(2025, 1, 10),
                prev_f_streak=None, prev_i_streak=None, rate_delay=0,
            )
        assert records[0].foreign_streak == 1
        assert records[1].foreign_streak == 2

    def test_returns_empty_when_no_rows(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "fetch_raw", return_value=[]):
            records = fetcher.fetch_records(
                "005930.KS", date(2025, 1, 1), date(2025, 1, 5),
                prev_f_streak=None, prev_i_streak=None, rate_delay=0,
            )
        assert records == []

    def test_alternative_date_key_trd_dd(self):
        """fetch_records handles slash-formatted TRD_DD from KRX API."""
        fetcher = _bare_fetcher()
        rows = [{"TRD_DD": "2025/01/03", "TRDVAL1": "300", "TRDVAL2": "100"}]
        with patch.object(fetcher, "fetch_raw", return_value=rows):
            records = fetcher.fetch_records(
                "005930.KS", date(2025, 1, 1), date(2025, 1, 5),
                prev_f_streak=None, prev_i_streak=None, rate_delay=0,
            )
        assert len(records) == 1
        assert records[0].foreign_net == 300

    def test_rows_outside_date_range_excluded(self):
        fetcher = _bare_fetcher()
        rows = (
            self._rows("20241231", "100", "50") +   # before start
            self._rows("20250103", "200", "80") +   # in range
            self._rows("20250201", "300", "60")     # after end
        )
        with patch.object(fetcher, "fetch_raw", return_value=rows):
            records = fetcher.fetch_records(
                "005930.KS", date(2025, 1, 1), date(2025, 1, 31),
                prev_f_streak=None, prev_i_streak=None, rate_delay=0,
            )
        assert len(records) == 1
        assert records[0].trade_date == date(2025, 1, 3)

    def test_yfinance_suffix_stripped_for_fetch_raw(self):
        """005930.KS → fetch_raw is called with '005930', not '005930.KS'."""
        fetcher = _bare_fetcher()
        captured = {}

        def capture(krx_code, start, end, isuCd):
            captured["krx_code"] = krx_code
            return []

        with patch.object(fetcher, "fetch_raw", side_effect=capture):
            fetcher.fetch_records(
                "005930.KS", date(2025, 1, 1), date(2025, 1, 5),
                prev_f_streak=None, prev_i_streak=None, rate_delay=0,
            )

        assert captured["krx_code"] == "005930"


# ── _last_trading_day() ───────────────────────────────────────────────────────
# --incremental은 평일에만 실행된다(cron mon-fri). 월요일 실행 시 '어제'를
# 그대로 쓰면 일요일이 되어, 금요일 거래일 데이터를 어떤 실행도 대상으로
# 삼지 못하고 영구히 누락시킨다 (토요일 실행이 없어 받아줄 회차가 없음).

class TestLastTradingDay:
    def test_monday_skips_back_to_friday(self):
        monday = date(2026, 6, 22)
        assert _last_trading_day(monday) == date(2026, 6, 19)  # Fri

    def test_tuesday_through_friday_use_plain_yesterday(self):
        for today_str, expected_str in [
            ("2026-06-23", "2026-06-22"),  # Tue -> Mon
            ("2026-06-24", "2026-06-23"),  # Wed -> Tue
            ("2026-06-25", "2026-06-24"),  # Thu -> Wed
            ("2026-06-26", "2026-06-25"),  # Fri -> Thu
        ]:
            today = date.fromisoformat(today_str)
            assert _last_trading_day(today) == date.fromisoformat(expected_str)

    def test_never_returns_a_weekend(self):
        for offset in range(14):
            today = date(2026, 6, 8) + timedelta(days=offset)
            assert _last_trading_day(today).weekday() < 5


# ── _fetch_kiwoom_records() (ka10045 backend) ─────────────────────────────────

class TestFetchKiwoomRecords:
    def _row(self, dt: str, orgn: str, for_: str) -> dict:
        return {"dt": dt, "orgn_daly_nettrde_qty": orgn, "for_daly_nettrde_qty": for_}

    def _client(self, pages: list[tuple[dict, dict]]) -> MagicMock:
        client = MagicMock()
        client._post.side_effect = pages
        return client

    def test_parses_inst_and_foreign_net(self):
        client = self._client([
            ({"stk_orgn_trde_trnsn": [self._row("20260619", "-1380348", "-3405260")]}, {}),
        ])
        records = _fetch_kiwoom_records(
            client, "005930.KS", date(2026, 6, 19), date(2026, 6, 19),
            prev_f_streak=None, prev_i_streak=None,
        )
        assert len(records) == 1
        rec = records[0]
        assert rec.ticker == "005930.KS"
        assert rec.trade_date == date(2026, 6, 19)
        assert rec.inst_net == -1380348
        assert rec.foreign_net == -3405260
        assert rec.personal_net is None  # ka10045 doesn't provide personal investor data

    def test_strips_yfinance_suffix_for_stk_cd(self):
        client = self._client([({"stk_orgn_trde_trnsn": []}, {})])
        _fetch_kiwoom_records(
            client, "005930.KS", date(2026, 1, 1), date(2026, 1, 5),
            prev_f_streak=None, prev_i_streak=None,
        )
        sent_body = client._post.call_args[0][2]
        assert sent_body["stk_cd"] == "005930"

    def test_returns_empty_when_no_rows(self):
        client = self._client([({"stk_orgn_trde_trnsn": []}, {})])
        records = _fetch_kiwoom_records(
            client, "005930.KS", date(2026, 1, 1), date(2026, 1, 5),
            prev_f_streak=None, prev_i_streak=None,
        )
        assert records == []

    def test_rows_outside_date_range_excluded(self):
        client = self._client([({"stk_orgn_trde_trnsn": [
            self._row("20251231", "100", "50"),    # before start
            self._row("20260103", "200", "80"),    # in range
            self._row("20260201", "300", "60"),    # after end
        ]}, {})])
        records = _fetch_kiwoom_records(
            client, "005930.KS", date(2026, 1, 1), date(2026, 1, 31),
            prev_f_streak=None, prev_i_streak=None,
        )
        assert len(records) == 1
        assert records[0].trade_date == date(2026, 1, 3)

    def test_streak_accumulates_in_ascending_date_order(self):
        # Real API returns rows newest-first; streak calc must sort ascending first.
        client = self._client([({"stk_orgn_trde_trnsn": [
            self._row("20260106", "500", "300"),
            self._row("20260103", "1000", "200"),
        ]}, {})])
        records = _fetch_kiwoom_records(
            client, "005930.KS", date(2026, 1, 1), date(2026, 1, 10),
            prev_f_streak=None, prev_i_streak=None,
        )
        assert records[0].trade_date == date(2026, 1, 3)
        assert records[0].foreign_streak == 1
        assert records[1].trade_date == date(2026, 1, 6)
        assert records[1].foreign_streak == 2

    def test_pagination_follows_cont_yn_next_key(self):
        client = self._client([
            ({"stk_orgn_trde_trnsn": [self._row("20260102", "100", "50")]},
             {"cont-yn": "Y", "next-key": "page2"}),
            ({"stk_orgn_trde_trnsn": [self._row("20260103", "200", "80")]},
             {"cont-yn": "N", "next-key": ""}),
        ])
        records = _fetch_kiwoom_records(
            client, "005930.KS", date(2026, 1, 1), date(2026, 1, 31),
            prev_f_streak=None, prev_i_streak=None,
        )
        assert len(records) == 2
        assert client._post.call_count == 2
        # second call must carry forward the next-key from the first response
        second_call_args = client._post.call_args_list[1][0]
        assert second_call_args[3] == "Y"
        assert second_call_args[4] == "page2"

    def test_stops_pagination_without_next_key(self):
        client = self._client([
            ({"stk_orgn_trde_trnsn": [self._row("20260102", "100", "50")]},
             {"cont-yn": "Y", "next-key": ""}),  # cont-yn=Y but no key → must not loop forever
        ])
        records = _fetch_kiwoom_records(
            client, "005930.KS", date(2026, 1, 1), date(2026, 1, 31),
            prev_f_streak=None, prev_i_streak=None,
        )
        assert len(records) == 1
        assert client._post.call_count == 1


# ── load_csv_records() ────────────────────────────────────────────────────────

class TestLoadCsvRecords:
    def _tmp_csv(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w", encoding="utf-8-sig") as f:
            f.write(content)
        return path

    def test_standard_format(self):
        path = self._tmp_csv("date,ticker,foreign_net,inst_net\n2025-01-03,005930.KS,1000,-500\n")
        try:
            records = load_csv_records(path)
        finally:
            os.unlink(path)
        assert len(records) == 1
        rec = records[0]
        assert rec.ticker == "005930.KS"
        assert rec.trade_date == date(2025, 1, 3)
        assert rec.foreign_net == 1000
        assert rec.inst_net == -500

    def test_standard_format_yyyymmdd(self):
        path = self._tmp_csv("date,ticker,foreign_net,inst_net\n20250103,005930.KS,500,-200\n")
        try:
            records = load_csv_records(path)
        finally:
            os.unlink(path)
        assert records[0].trade_date == date(2025, 1, 3)

    def test_krx_style_format(self):
        content = "날짜,종목코드,종목명,외국인순매수,기관합계\n20250103,005930,삼성전자,1234,-567\n"
        path = self._tmp_csv(content)
        try:
            records = load_csv_records(path)
        finally:
            os.unlink(path)
        assert len(records) == 1
        rec = records[0]
        assert rec.trade_date == date(2025, 1, 3)
        assert rec.foreign_net == 1234
        assert rec.inst_net == -567
        assert rec.ticker == "005930.KS"

    def test_skips_invalid_dates(self):
        path = self._tmp_csv("date,ticker,foreign_net,inst_net\nBAD,005930.KS,100,50\n")
        try:
            records = load_csv_records(path)
        finally:
            os.unlink(path)
        assert records == []

    def test_multiple_rows(self):
        content = (
            "date,ticker,foreign_net,inst_net\n"
            "2025-01-02,005930.KS,100,50\n"
            "2025-01-03,005930.KS,200,-80\n"
        )
        path = self._tmp_csv(content)
        try:
            records = load_csv_records(path)
        finally:
            os.unlink(path)
        assert len(records) == 2
        assert records[0].trade_date == date(2025, 1, 2)
        assert records[1].trade_date == date(2025, 1, 3)


# ── _compute_streaks() ────────────────────────────────────────────────────────

class TestComputeStreaks:
    def test_ascending_streak(self):
        records = [
            FlowRecord("A.KS", date(2025, 1, 1), 100, 50),
            FlowRecord("A.KS", date(2025, 1, 2), 200, 30),
            FlowRecord("A.KS", date(2025, 1, 3), 150, 20),
        ]
        result = _compute_streaks(records)
        f_streaks = [r.foreign_streak for r in result]
        assert f_streaks == [1, 2, 3]

    def test_streak_resets_on_reversal(self):
        records = [
            FlowRecord("A.KS", date(2025, 1, 1), 100, None),
            FlowRecord("A.KS", date(2025, 1, 2), 200, None),
            FlowRecord("A.KS", date(2025, 1, 3), -50, None),
        ]
        result = _compute_streaks(records)
        assert result[2].foreign_streak == -1

    def test_two_tickers_independent(self):
        records = [
            FlowRecord("A.KS", date(2025, 1, 1), 100, None),
            FlowRecord("A.KS", date(2025, 1, 2), 200, None),
            FlowRecord("B.KQ", date(2025, 1, 1), -50, None),
            FlowRecord("B.KQ", date(2025, 1, 2), -100, None),
        ]
        result = _compute_streaks(records)
        a = sorted([r for r in result if r.ticker == "A.KS"], key=lambda r: r.trade_date)
        b = sorted([r for r in result if r.ticker == "B.KQ"], key=lambda r: r.trade_date)
        assert a[1].foreign_streak == 2
        assert b[1].foreign_streak == -2


# ── _jittered_delay() ────────────────────────────────────────────────────────
# Added for the Tor anti-blocking measures (/plan-eng-review, 2026-07-11):
# request spacing gets randomized under Tor so KRX can't fingerprint a fixed
# 0.5s cadence as bot traffic.

class TestJitteredDelay:
    def test_returns_base_unchanged_when_tor_proxy_unset(self, monkeypatch):
        monkeypatch.delenv("TOR_PROXY", raising=False)
        assert _jittered_delay(0.5) == 0.5

    def test_randomizes_within_bounds_when_tor_proxy_set(self, monkeypatch):
        monkeypatch.setenv("TOR_PROXY", "socks5h://127.0.0.1:9150")
        for _ in range(50):
            result = _jittered_delay(0.5)
            assert 1.0 <= result <= 2.5

    def test_floor_is_one_second_even_for_zero_base(self, monkeypatch):
        """rate_delay could theoretically be 0 — jitter must not collapse to 0."""
        monkeypatch.setenv("TOR_PROXY", "socks5h://127.0.0.1:9150")
        result = _jittered_delay(0.0)
        assert result >= 1.0

    def test_base_above_floor_shifts_the_range(self, monkeypatch):
        monkeypatch.setenv("TOR_PROXY", "socks5h://127.0.0.1:9150")
        for _ in range(50):
            result = _jittered_delay(2.0)
            assert 2.0 <= result <= 3.5


# ── _tor_new_identity() ───────────────────────────────────────────────────────
# Tor control-port SIGNAL NEWNYM via stem (auto-discovers the control cookie
# through PROTOCOLINFO — no manual cookie path needed). Best-effort: any
# failure (control port down, bad auth, connection refused) must return
# False, never raise — callers use this in exception handlers and must not
# have a *second* exception thrown at them from the recovery path itself.

class TestTorNewIdentity:
    def _mock_controller(self):
        """MagicMock usable as `with Controller.from_port(...) as controller:`."""
        controller = MagicMock()
        controller.__enter__ = MagicMock(return_value=controller)
        controller.__exit__ = MagicMock(return_value=False)
        return controller

    def test_returns_true_on_successful_auth_and_signal(self, monkeypatch):
        monkeypatch.delenv("TOR_CONTROL_PORT", raising=False)
        controller = self._mock_controller()
        with patch("stem.control.Controller.from_port", return_value=controller) as mock_from_port:
            result = _tor_new_identity()

        assert result is True
        mock_from_port.assert_called_once_with(port=9151)
        controller.authenticate.assert_called_once_with()
        controller.signal.assert_called_once()

    def test_uses_configured_control_port(self, monkeypatch):
        monkeypatch.setenv("TOR_CONTROL_PORT", "9051")
        controller = self._mock_controller()
        with patch("stem.control.Controller.from_port", return_value=controller) as mock_from_port:
            _tor_new_identity()
        mock_from_port.assert_called_once_with(port=9051)

    def test_returns_false_on_auth_failure(self):
        controller = self._mock_controller()
        controller.authenticate.side_effect = Exception("authentication failed")
        with patch("stem.control.Controller.from_port", return_value=controller):
            result = _tor_new_identity()  # must not raise
        assert result is False
        controller.signal.assert_not_called()

    def test_returns_false_on_connection_error(self):
        import stem
        with patch("stem.control.Controller.from_port", side_effect=stem.SocketError("refused")):
            result = _tor_new_identity()  # must not raise
        assert result is False


# ── fetch_raw() 403 rotation-retry ────────────────────────────────────────────
# KRX appears to blocklist a large share of Tor exit IPs (/plan-eng-review
# investigation, 2026-07-10). On 403, fetch_raw rotates the Tor circuit and
# retries the same request once before giving up.

class TestFetchRaw403Retry:
    def _http_error(self, status_code: int):
        import requests
        resp = MagicMock()
        resp.status_code = status_code
        return requests.HTTPError(response=resp)

    def test_403_with_successful_rotation_retries_and_succeeds(self):
        fetcher = _bare_fetcher()
        rows = [{"TRD_DD": "20250103", "TRDVAL1": "100"}]
        success_bytes = json.dumps({"output": rows}).encode("utf-8")

        with patch.object(fetcher, "_post", side_effect=[self._http_error(403), success_bytes]) as mock_post, \
             patch("data.krx_flow_sync.new_identity", return_value=True) as mock_rotate, \
             patch("data.krx_flow_sync._time.sleep"):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))

        assert result == rows
        assert mock_post.call_count == 2
        mock_rotate.assert_called_once()

    def test_403_with_failed_rotation_returns_empty(self):
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", side_effect=self._http_error(403)) as mock_post, \
             patch("data.krx_flow_sync.new_identity", return_value=False):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))

        assert result == []
        mock_post.assert_called_once()  # no retry when rotation itself fails

    def test_403_retry_also_fails_returns_empty(self):
        fetcher = _bare_fetcher()
        with patch.object(
            fetcher, "_post", side_effect=[self._http_error(403), self._http_error(403)]
        ) as mock_post, \
             patch("data.krx_flow_sync.new_identity", return_value=True), \
             patch("data.krx_flow_sync._time.sleep"):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))

        assert result == []
        assert mock_post.call_count == 2

    def test_non_403_exception_does_not_attempt_rotation(self):
        """Plain connection errors (no .response) must not trigger rotation."""
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", side_effect=ConnectionError("down")) as mock_post, \
             patch("data.krx_flow_sync.new_identity") as mock_rotate:
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))

        assert result == []
        mock_post.assert_called_once()
        mock_rotate.assert_not_called()

    def test_500_error_does_not_attempt_rotation(self):
        """Only 403 triggers rotation — other HTTP errors shouldn't burn a circuit."""
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", side_effect=self._http_error(500)), \
             patch("data.krx_flow_sync.new_identity") as mock_rotate:
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))

        assert result == []
        mock_rotate.assert_not_called()

    def _http_error_with_body(self, status_code: int, body: bytes):
        import requests
        resp = MagicMock()
        resp.status_code = status_code
        resp.content = body
        return requests.HTTPError(response=resp)

    def test_400_logout_body_warns_once_and_returns_empty(self, caplog):
        """KRX returns the LOGOUT auth-failure body wrapped in a 400 status
        (2026-07-13 investigation) instead of 200. raise_for_status() raises
        before the plain 'raw in (b"LOGOUT", b"")' check ever runs, so this
        must be detected from the exception's response body — and only
        warned once per fetcher instance to avoid spamming per-ticker."""
        import logging
        fetcher = _bare_fetcher()
        err = self._http_error_with_body(400, b"LOGOUT")
        with patch.object(fetcher, "_post", side_effect=[err, err]), \
             caplog.at_level(logging.WARNING, logger="data.krx_flow_sync"):
            result1 = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))
            result2 = fetcher.fetch_raw("000660", date(2025, 1, 1), date(2025, 1, 5))

        assert result1 == [] and result2 == []
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1  # second occurrence stays quiet
        assert "세션 만료" in warnings[0].message

    def test_connection_error_without_response_does_not_warn_logout(self, caplog):
        """A bare connection error (no .response at all) must not be
        misclassified as a LOGOUT body — it has no body to inspect."""
        import logging
        fetcher = _bare_fetcher()
        with patch.object(fetcher, "_post", side_effect=ConnectionError("down")), \
             caplog.at_level(logging.WARNING, logger="data.krx_flow_sync"):
            result = fetcher.fetch_raw("005930", date(2025, 1, 1), date(2025, 1, 5))

        assert result == []
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]


# ── _handle_possible_expiry() ─────────────────────────────────────────────────

class TestHandlePossibleExpiry:
    @pytest.mark.asyncio
    async def test_below_threshold_returns_false_without_probing(self):
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        with patch.object(fetcher, "fetch_raw") as mock_fetch:
            result = await _handle_possible_expiry(fetcher, consecutive_empty=1)
        assert result is False
        mock_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_exhausted_flag_short_circuits_regardless_of_count(self):
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = True
        with patch.object(fetcher, "fetch_raw") as mock_fetch:
            result = await _handle_possible_expiry(fetcher, consecutive_empty=999)
        assert result is False
        mock_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_probe_returns_rows_session_normal(self):
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        rows = [{"TRD_DD": "20250103", "TRDVAL1": "100"}]
        with patch.object(fetcher, "fetch_raw", return_value=rows):
            result = await _handle_possible_expiry(fetcher, consecutive_empty=5)
        assert result is False
        assert fetcher._session_wait_exhausted is False

    @pytest.mark.asyncio
    async def test_rotation_recovers_session(self):
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        rows = [{"TRD_DD": "20250103", "TRDVAL1": "100"}]
        with patch.object(fetcher, "fetch_raw", side_effect=[[], rows]), \
             patch("data.krx_flow_sync.new_identity", return_value=True), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await _handle_possible_expiry(fetcher, consecutive_empty=5)
        assert result is True
        assert fetcher._session_wait_exhausted is False

    @pytest.mark.asyncio
    async def test_auto_relogin_recovers_session_when_rotation_fails(self):
        """/plan-eng-review D6: with KRX_ID/KRX_PW available, attempt login()
        (now that it works — mbrId/pw fix) before falling to the 30-min
        manual-cookie wait."""
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        rows = [{"TRD_DD": "20250103", "TRDVAL1": "100"}]
        with patch.object(fetcher, "fetch_raw", side_effect=[[], rows]), \
             patch.object(fetcher, "login", return_value=True) as mock_login, \
             patch("data.krx_flow_sync.new_identity", return_value=False), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await _handle_possible_expiry(
                fetcher, consecutive_empty=5, krx_id="testuser", krx_pw="testpw"
            )
        assert result is True
        assert fetcher._session_wait_exhausted is False
        mock_login.assert_called_once_with("testuser", "testpw")

    @pytest.mark.asyncio
    async def test_auto_relogin_skipped_when_credentials_missing(self):
        """No KRX_ID/KRX_PW → login() must not be attempted, falls straight
        through to manual wait (which times out here)."""
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        with patch.object(fetcher, "fetch_raw", return_value=[]), \
             patch.object(fetcher, "login") as mock_login, \
             patch("data.krx_flow_sync.new_identity", return_value=False), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("data.krx_flow_sync.refresh_env"):
            result = await _handle_possible_expiry(
                fetcher, consecutive_empty=5, krx_id=None, krx_pw=None
            )
        assert result is False
        mock_login.assert_not_called()

    @pytest.mark.asyncio
    async def test_auto_relogin_failure_falls_through_to_manual_wait(self, monkeypatch):
        """login() succeeds at the HTTP layer but the post-login probe is still
        empty (e.g. CD011 duplicate-login edge case) → must still fall
        through to the timeout path, not silently return True."""
        monkeypatch.delenv("KRX_SESSION", raising=False)
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        with patch.object(fetcher, "fetch_raw", return_value=[]), \
             patch.object(fetcher, "login", return_value=False), \
             patch("data.krx_flow_sync.new_identity", return_value=False), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("data.krx_flow_sync.refresh_env"):
            result = await _handle_possible_expiry(
                fetcher, consecutive_empty=5, krx_id="testuser", krx_pw="testpw"
            )
        assert result is False
        assert fetcher._session_wait_exhausted is True

    @pytest.mark.asyncio
    async def test_terminal_login_error_raises_instead_of_manual_wait(self, monkeypatch):
        """2026-09-07 incident: CD010 ("패스워드 변경 필요") on the auto-relogin
        attempt must abort immediately with KrxTerminalAuthError — not fall
        through to the 30-min KRX_SESSION wait (which can never succeed for
        a password-change-required account) and not go through the
        Tor-rotation retry loop for every remaining ticker afterwards."""
        monkeypatch.delenv("KRX_SESSION", raising=False)
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False

        def _failing_login(_id, _pw):
            fetcher.last_error_code = "CD010"
            fetcher.last_error_message = "패스워드 변경 필요"
            return False

        with patch.object(fetcher, "fetch_raw", return_value=[]), \
             patch.object(fetcher, "login", side_effect=_failing_login), \
             patch("data.krx_flow_sync.new_identity", return_value=False), \
             patch("asyncio.sleep", new=AsyncMock()) as mock_sleep, \
             patch("data.krx_flow_sync.refresh_env"):
            with pytest.raises(KrxTerminalAuthError) as exc_info:
                await _handle_possible_expiry(
                    fetcher, consecutive_empty=5, krx_id="testuser", krx_pw="testpw"
                )

        assert exc_info.value.err_code == "CD010"
        assert fetcher._session_wait_exhausted is True
        # The 30-min manual-wait loop sleeps in 30s steps — none of those
        # must have run, since we should have raised before reaching it.
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_timeout_sets_exhausted_flag_and_returns_false(self, monkeypatch):
        """No KRX_SESSION refresh ever arrives — must give up, not hang forever
        (regression test: an unbounded wait here previously could wedge
        daily_flow_sync_job permanently since it has max_instances=1)."""
        monkeypatch.delenv("KRX_SESSION", raising=False)
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        with patch.object(fetcher, "fetch_raw", return_value=[]), \
             patch("data.krx_flow_sync.new_identity", return_value=False), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("data.krx_flow_sync.refresh_env"):
            result = await _handle_possible_expiry(fetcher, consecutive_empty=5)
        assert result is False
        assert fetcher._session_wait_exhausted is True

    @pytest.mark.asyncio
    async def test_session_refresh_during_wait_resets_and_returns_true(self, monkeypatch):
        fetcher = _bare_fetcher()
        fetcher._session_wait_exhausted = False
        monkeypatch.setenv("KRX_SESSION", "old-session")
        monkeypatch.delenv("KRX_VISITOR", raising=False)

        call_count = [0]

        def fake_refresh_env(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                monkeypatch.setenv("KRX_SESSION", "new-session")

        with patch.object(fetcher, "fetch_raw", return_value=[]), \
             patch.object(fetcher, "inject_session") as mock_inject, \
             patch("data.krx_flow_sync.new_identity", return_value=False), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("data.krx_flow_sync.refresh_env", side_effect=fake_refresh_env):
            result = await _handle_possible_expiry(fetcher, consecutive_empty=5)

        assert result is True
        assert fetcher._session_wait_exhausted is False
        mock_inject.assert_called_once_with("new-session", None)


# ── _make_krx_direct() terminal login errors ──────────────────────────────────
# 2026-09-07 incident: CD010 ("패스워드 변경 필요") on the very first login
# attempt was silently ignored (return value never checked) — run_krx_direct
# then spent ~2h26m fetching every ticker, misdiagnosing each empty response
# as a recoverable session expiry, before exiting 1. _make_krx_direct must
# now abort immediately for error codes we know can't be fixed by retrying.

class TestMakeKrxDirect:
    def test_terminal_login_error_raises_immediately(self):
        fetcher = _bare_fetcher()

        def _fail(krx_id, krx_pw):
            fetcher.last_error_code = "CD010"
            fetcher.last_error_message = "패스워드 변경 필요"
            return False

        with patch("data.krx_flow_sync._KrxDirectFetcher", return_value=fetcher), \
             patch.object(fetcher, "warmup", return_value=True), \
             patch.object(fetcher, "login", side_effect=_fail):
            with pytest.raises(KrxTerminalAuthError) as exc_info:
                _make_krx_direct("user", "pw")

        assert exc_info.value.err_code == "CD010"
        assert exc_info.value.err_msg == "패스워드 변경 필요"

    def test_non_terminal_login_failure_does_not_raise(self):
        """CD003 (bad credentials) is a real failure but not in the terminal
        set — _make_krx_direct's existing behavior (proceed with an
        unauthenticated fetcher, let per-ticker handling deal with it) must
        stay unchanged for codes not explicitly classified as terminal."""
        fetcher = _bare_fetcher()

        def _fail(krx_id, krx_pw):
            fetcher.last_error_code = "CD003"
            fetcher.last_error_message = "service error"
            return False

        with patch("data.krx_flow_sync._KrxDirectFetcher", return_value=fetcher), \
             patch.object(fetcher, "warmup", return_value=True), \
             patch.object(fetcher, "login", side_effect=_fail):
            result = _make_krx_direct("user", "pw")

        assert result is fetcher
        assert fetcher.last_error_code == "CD003"

    def test_successful_login_does_not_raise(self):
        fetcher = _bare_fetcher()
        with patch("data.krx_flow_sync._KrxDirectFetcher", return_value=fetcher), \
             patch.object(fetcher, "warmup", return_value=True), \
             patch.object(fetcher, "login", return_value=True):
            result = _make_krx_direct("user", "pw")

        assert result is fetcher


# ── run_krx_direct() return value ─────────────────────────────────────────────
# Regression test for /plan-eng-review D5: run_krx_direct previously returned
# None, so main() could not tell "collected nothing" from "collected
# everything" and always exited 0 — meaning daily_flow_sync_job's Telegram
# failure alert never fired for the exact case it exists to catch (Tor
# Browser closed → every request fails to connect at all, no 403, fetch_raw
# just returns [] silently, main() saw exit=0 and stayed quiet).

class TestRunKrxDirectReturnValue:
    def _patch_common(self, tickers, fetch_records_return):
        """Common patch set: ticker universe, DB helpers, and a fetcher whose
        fetch_records is fully controlled by the test."""
        fetcher = _bare_fetcher()
        return [
            patch("analysis.chart_screener.get_all_tickers", return_value=tickers),
            patch("data.krx_flow_sync._make_krx_direct", return_value=fetcher),
            patch("data.krx_flow_sync._get_existing_dates", new=AsyncMock(return_value=set())),
            patch("core.db.get_prev_streak", new=AsyncMock(return_value=(None, None, None))),
            patch.object(fetcher, "fetch_records", side_effect=fetch_records_return),
        ], fetcher

    @pytest.mark.asyncio
    async def test_returns_zero_when_every_ticker_fetch_fails(self):
        """All connections fail (e.g. Tor Browser closed) → fetch_records
        returns [] for every ticker → run_krx_direct must return 0, not None,
        so main() can detect the failure and exit non-zero."""
        tickers = [("005930.KS", "삼성전자", ""), ("000660.KS", "SK하이닉스", "")]
        patches, fetcher = self._patch_common(tickers, fetch_records_return=[[], []])
        pool = MagicMock()

        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            result = await run_krx_direct(
                pool, date(2025, 1, 1), date(2025, 1, 5), "ALL", 0,
                krx_id=None, krx_pw=None, krx_session="fake-session",
            )

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_total_saved_count_on_success(self):
        tickers = [("005930.KS", "삼성전자", ""), ("000660.KS", "SK하이닉스", "")]
        rows_a = [FlowRecord("005930.KS", date(2025, 1, 3), 100, 50)]
        rows_b = [FlowRecord("000660.KS", date(2025, 1, 3), -200, 30)]
        patches, fetcher = self._patch_common(tickers, fetch_records_return=[rows_a, rows_b])
        pool = MagicMock()

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patch("data.krx_flow_sync._save_batch", new=AsyncMock(side_effect=[1, 1])):
            result = await run_krx_direct(
                pool, date(2025, 1, 1), date(2025, 1, 5), "ALL", 0,
                krx_id=None, krx_pw=None, krx_session="fake-session",
            )

        assert result == 2


# ── run_krx_direct() ticker order shuffle ─────────────────────────────────────
# 2026-07-14 investigation: KRX appears to cut a session off after a fixed
# request-count budget regardless of Tor circuit/relogin — 07-10 and 07-13
# both collapsed at the exact same list index (945/2766). Processing tickers
# in the same fixed order every day means the same trailing ~1800 tickers
# are permanently starved. Shuffling means a different slice gets covered
# each day so full coverage accumulates over time instead of never.

class TestRunKrxDirectShuffle:
    @pytest.mark.asyncio
    async def test_shuffles_ticker_order_before_processing(self):
        # Stay below _SESSION_EXPIRY_THRESHOLD(5) consecutive empties — at or
        # above it, run_krx_direct calls the real _handle_possible_expiry(),
        # which (with no krx_id/krx_pw to retry) falls through to the genuine
        # 30-minute KRX_SESSION wait loop (asyncio.sleep(30) x60) and actually
        # blocks the test for real wall-clock minutes instead of failing fast.
        tickers = [(f"{i:06d}.KS", f"종목{i}", "") for i in range(4)]
        patches, fetcher = TestRunKrxDirectReturnValue()._patch_common(
            tickers, fetch_records_return=[[] for _ in tickers]
        )
        pool = MagicMock()

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patch("data.krx_flow_sync.random.shuffle", wraps=lambda lst: lst.reverse()) as mock_shuffle:
            await run_krx_direct(
                pool, date(2025, 1, 1), date(2025, 1, 5), "ALL", 0,
                krx_id=None, krx_pw=None, krx_session="fake-session",
            )

        mock_shuffle.assert_called_once()
