from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
import pytz

import trade.pnl_monitor as pnl_monitor


@pytest.fixture(autouse=True)
def dummy_client(monkeypatch):
    """Replace the BigQuery client with a MagicMock for each test."""
    client = MagicMock()
    # Default .query().to_dataframe() to empty DataFrame
    client.query.return_value.to_dataframe.return_value = pd.DataFrame()
    monkeypatch.setattr(pnl_monitor, "CLIENT", client)
    return client


@pytest.fixture
def sample_leg_df():
    """Sample DataFrame representing one open trade leg."""
    return pd.DataFrame(
        [
            {
                "trade_id": "T1",
                "leg_id": "L1",
                "strike": 100.0,
                "leg_type": "call",
                "direction": "long",
                "entry_price": 5.0,
                "expiration_date": pd.Timestamp("2025-05-15").date(),
            }
        ]
    )


# ---------- Intraday & basic branches ----------


def test_no_open_legs(dummy_client):
    """
    No open legs => function returns early w/o any insert.
    """
    dummy_client.query.return_value.to_dataframe.return_value = pd.DataFrame()
    pnl_monitor.update_trade_pnl("SPX", quote={"last": 100}, mid_maps={})
    dummy_client.insert_rows_json.assert_not_called()


def test_missing_quote(dummy_client, sample_leg_df):
    """
    Open legs but missing quote => skip PnL update.
    """
    dummy_client.query.return_value.to_dataframe.return_value = sample_leg_df
    pnl_monitor.update_trade_pnl("SPX", quote=None, mid_maps={})
    dummy_client.insert_rows_json.assert_not_called()


def test_intraday_pnl(monkeypatch, dummy_client, sample_leg_df):
    """
    Intraday: compute raw_pnl in points, *100, status='open'.
    """
    dummy_client.query.return_value.to_dataframe.return_value = sample_leg_df
    # Freeze time to 2025-05-10 12:00 UTC => ET ~08:00
    fixed = datetime(2025, 5, 10, 12, 0, tzinfo=timezone.utc)

    class DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(pnl_monitor, "datetime", DT)

    quote = {"last": 110.0}
    mid_maps = {sample_leg_df.iloc[0]["expiration_date"]: {(100.0, "call"): 7.0}}
    pnl_monitor.update_trade_pnl("SPX", quote=quote, mid_maps=mid_maps)

    # Expect theoretical_pnl=2.0, status open
    dummy_client.insert_rows_json.assert_called_once()
    _, rows = dummy_client.insert_rows_json.call_args[0]
    r = rows[0]
    assert r["theoretical_pnl"] == 2.0
    assert r["status"] == "open"

    # Both trade_legs and trade_recommendations updates
    updates = [c for c in dummy_client.query.call_args_list if "UPDATE" in c[0][0]]
    assert any("trade_legs" in c[0][0] for c in updates)
    assert any("trade_recommendations" in c[0][0] for c in updates)


def test_mid_map_fallback_to_entry_price(monkeypatch, dummy_client, sample_leg_df):
    """
    No mid_map entry => current=entry_price => raw_pnl=0.
    """
    dummy_client.query.return_value.to_dataframe.return_value = sample_leg_df
    # Freeze intraday
    fixed = datetime(2025, 5, 10, 12, 0, tzinfo=timezone.utc)

    class DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(pnl_monitor, "datetime", DT)

    quote = {"last": 110.0}
    pnl_monitor.update_trade_pnl("SPX", quote=quote, mid_maps={})
    _, rows = dummy_client.insert_rows_json.call_args[0]
    assert rows[0]["theoretical_pnl"] == 0.0


# ---------- Multi‑leg roll‑up intraday ----------


def test_multileg_rollup_intraday(monkeypatch, dummy_client):
    """
    Two legs same trade_id => sum raw_pnl then *100.
    Leg1: long call entry 5, mid 7 => +2
    Leg2: short put entry 2, mid 1 => +1
    Total pts=3 => pnl=300
    """
    df = pd.DataFrame(
        [
            {
                "trade_id": "T1",
                "leg_id": "L1",
                "strike": 100.0,
                "leg_type": "call",
                "direction": "long",
                "entry_price": 5.0,
                "expiration_date": pd.Timestamp("2025-05-15").date(),
            },
            {
                "trade_id": "T1",
                "leg_id": "L2",
                "strike": 100.0,
                "leg_type": "put",
                "direction": "short",
                "entry_price": 2.0,
                "expiration_date": pd.Timestamp("2025-05-15").date(),
            },
        ]
    )
    dummy_client.query.return_value.to_dataframe.return_value = df
    fixed = datetime(2025, 5, 10, 12, 0, tzinfo=timezone.utc)

    class DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(pnl_monitor, "datetime", DT)

    mid_maps = {pd.Timestamp("2025-05-15").date(): {(100.0, "call"): 7.0, (100.0, "put"): 1.0}}
    pnl_monitor.update_trade_pnl("SPX", quote={"last": 110.0}, mid_maps=mid_maps)

    rec_calls = [c for c in dummy_client.query.call_args_list if "trade_recommendations" in c[0][0]]
    # last rec update
    job = rec_calls[-1][1]["job_config"]
    params = {p.name: p.value for p in job.query_parameters}
    assert params["pnl"] == 300.0


# ---------- EOD branches ----------


def test_eod_pnl_closed(monkeypatch, dummy_client, sample_leg_df):
    """
    EOD with P/L analysis present => use max_profit/max_loss logic.
    """
    legs = sample_leg_df
    pl_df = pd.DataFrame([{"max_profit": 100.0, "max_loss": -50.0}])
    info_df = pd.DataFrame(
        [
            {"direction": "short", "leg_type": "put", "strike": 95.0},
            {"direction": "short", "leg_type": "call", "strike": 105.0},
        ]
    )
    # stub 5 queries: legs, upd legs, pl_df, info_df, upd recs
    dummy_client.query.side_effect = [
        MagicMock(to_dataframe=MagicMock(return_value=legs)),
        MagicMock(),
        MagicMock(to_dataframe=MagicMock(return_value=pl_df)),
        MagicMock(to_dataframe=MagicMock(return_value=info_df)),
        MagicMock(),
    ]
    # Freeze to ET16:01 => UTC20:01
    fixed = datetime(2025, 5, 10, 20, 1, tzinfo=timezone.utc)

    class DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(pnl_monitor, "datetime", DT)

    pnl_monitor.update_trade_pnl("SPX", quote={"last": 100.0}, mid_maps={})
    # snapshot status closed
    _, rows = dummy_client.insert_rows_json.call_args[0]
    assert rows[0]["status"] == "closed"
    # final pnl from max_profit
    rec = [c for c in dummy_client.query.call_args_list if "trade_recommendations" in c[0][0]][-1]
    params = {p.name: p.value for p in rec[1]["job_config"].query_parameters}
    assert params["pnl"] == 100.0 and params["status"] == "closed"


def test_eod_fallback_to_raw_sum(monkeypatch, dummy_client, sample_leg_df):
    """
    EOD with no P/L analysis => fallback to raw_sum*100.
    """
    legs = sample_leg_df
    # stub legs, upd legs, empty pl_df, upd recs
    dummy_client.query.side_effect = [
        MagicMock(to_dataframe=MagicMock(return_value=legs)),
        MagicMock(),
        MagicMock(to_dataframe=MagicMock(return_value=pd.DataFrame())),
        MagicMock(),
    ]
    fixed = datetime(2025, 5, 10, 20, 2, tzinfo=timezone.utc)

    class DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(pnl_monitor, "datetime", DT)

    # supply mid_map for one leg: pts = entry_price->mid diff = +1
    mid_maps = {sample_leg_df.iloc[0]["expiration_date"]: {(100.0, "call"): 6.0}}
    pnl_monitor.update_trade_pnl("SPX", quote={"last": 100.0}, mid_maps=mid_maps)

    rec = [c for c in dummy_client.query.call_args_list if "trade_recommendations" in c[0][0]][-1]
    params = {p.name: p.value for p in rec[1]["job_config"].query_parameters}
    # raw_pnl = 6-5=1 pts => *100 =100
    assert params["pnl"] == 100.0 and params["status"] == "closed"


# ---------- EOD time boundary ----------


def test_eod_time_boundary(monkeypatch, dummy_client, sample_leg_df):
    """
    16:04 ET = closed; 16:05 ET = intraday-open
    """
    # Pre-define P/L analysis DataFrames for EOD stub
    pl_df = pd.DataFrame([{"max_profit": 10.0, "max_loss": -20.0}])
    info_df = pd.DataFrame(
        [
            {"direction": "short", "leg_type": "put", "strike": 95.0},
            {"direction": "short", "leg_type": "call", "strike": 105.0},
        ]
    )

    for minute, expected in [(4, "closed"), (5, "open")]:
        # Reset mocks
        dummy_client.insert_rows_json.reset_mock()
        dummy_client.query.reset_mock()
        dummy_client.query.side_effect = None
        dummy_client.query.return_value = MagicMock(
            to_dataframe=MagicMock(return_value=sample_leg_df)
        )

        if expected == "closed":
            # For EOD path, override query to stub the sequence of calls
            dummy_client.query.side_effect = [
                MagicMock(to_dataframe=MagicMock(return_value=sample_leg_df)),  # select legs
                MagicMock(),  # update trade_legs
                MagicMock(to_dataframe=MagicMock(return_value=pl_df)),  # pl_analysis
                MagicMock(to_dataframe=MagicMock(return_value=info_df)),  # strike info
                MagicMock(),  # update trade_recommendations
            ]
        # Freeze time: ET 16:minute -> UTC 20:minute
        fixed = datetime(2025, 5, 10, 20, minute, tzinfo=timezone.utc)

        class DT(datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed

        monkeypatch.setattr(pnl_monitor, "datetime", DT)

        mid_maps = {sample_leg_df.iloc[0]["expiration_date"]: {(100.0, "call"): 7.0}}
        pnl_monitor.update_trade_pnl("SPX", quote={"last": 110.0}, mid_maps=mid_maps)

        # Check status in the live snapshot
        _, rows = dummy_client.insert_rows_json.call_args[0]
        assert rows[0]["status"] == expected, f"Minute {minute}: expected status {expected}"
        assert dummy_client.query.called
