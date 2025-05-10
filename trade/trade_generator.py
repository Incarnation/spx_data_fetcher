# trade/trade_generator.py
# =====================
# Multi‑Strategy Trade Generator (0DTE Iron Condor & Vertical Spreads)
# =====================

import logging
from datetime import date, datetime, timezone
from typing import Optional, Union

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from common.auth import get_gcp_credentials
from common.config import (
    GOOGLE_CLOUD_PROJECT,
    MODEL_VERSION,
    TARGET_DELTA,
    WING_WIDTH,
)
from trade.pl_analysis import compute_and_store_pl_analysis

# ── Table references ─────────────────────────────────────────────────────────
TRADE_RECS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_recommendations"
TRADE_LEGS = f"{GOOGLE_CLOUD_PROJECT}.analytics.trade_legs"
OPT_SNAP = f"{GOOGLE_CLOUD_PROJECT}.options.option_chain_snapshot"
IDX_PRICE = f"{GOOGLE_CLOUD_PROJECT}.market_data.index_price_snapshot"

# ── BigQuery client ───────────────────────────────────────────────────────────
CLIENT = bigquery.Client(
    credentials=get_gcp_credentials(),
    project=GOOGLE_CLOUD_PROJECT,
)


def _closest_strike(available: pd.Series, target: float) -> float:
    """
    Return the strike in `available` closest to `target`.
    """
    idx = (available - target).abs().idxmin()
    return available.loc[idx]


def generate_0dte_trade(
    symbol: str = "SPX",
    strategy_type: str = "iron_condor",
    expiry_date: Optional[Union[str, date]] = None,
):
    """
    1) Fetch latest index spot.
    2) Pull freshest options snapshot for a given expiry.
    3) Choose strikes for Iron Condor or Vertical Spread.
    4) Compute net entry_price from mid‑prices.
    5) Insert into trade_recommendations & trade_legs.
    6) Trigger P/L analysis (passing both symbol and root_symbol).
    """
    logging.info("🔎 Generating 0DTE %s (expiry=%s)…", strategy_type, expiry_date)

    # 0️⃣ Resolve expiry date string
    now_dt = datetime.now(timezone.utc)
    expiry = (
        expiry_date.isoformat()
        if isinstance(expiry_date, date)
        else (expiry_date or now_dt.date().isoformat())
    )
    timestamp = now_dt.isoformat()

    # 1️⃣ Fetch spot price from index_price_snapshot
    spot_sql = f"""
      SELECT last
      FROM `{IDX_PRICE}`
      WHERE symbol = @sym
      ORDER BY timestamp DESC
      LIMIT 1
    """
    spot_df = CLIENT.query(
        spot_sql,
        job_config=QueryJobConfig(query_parameters=[ScalarQueryParameter("sym", "STRING", symbol)]),
    ).to_dataframe()
    if spot_df.empty:
        logging.error("No spot price for %s; aborting.", symbol)
        return
    spot = float(spot_df["last"].iloc[0])

    # 2️⃣ Load freshest options for this expiry (root_symbol = symbol + 'W')
    root_sym = f"{symbol}W"
    opts_sql = f"""
      SELECT strike, option_type, bid, ask, delta
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY CAST(strike AS STRING), option_type
                 ORDER BY timestamp DESC
               ) AS rn
        FROM `{OPT_SNAP}`
        WHERE root_symbol     = @rsym
          AND expiration_date = @expiry
      )
      WHERE rn = 1
    """
    opts_df = CLIENT.query(
        opts_sql,
        job_config=QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("rsym", "STRING", root_sym),
                ScalarQueryParameter("expiry", "DATE", expiry),
            ]
        ),
    ).to_dataframe()
    if opts_df.empty:
        logging.error("No option chains for %s @ %s; aborting.", root_sym, expiry)
        return

    # 3️⃣ Build leg definitions
    puts = opts_df[opts_df.option_type == "put"].copy()
    calls = opts_df[opts_df.option_type == "call"].copy()

    if strategy_type == "iron_condor":
        if puts.empty or calls.empty:
            logging.error("Cannot build Iron Condor: missing puts or calls.")
            return
        # find strikes closest to TARGET_DELTA
        puts["dd"] = (puts.delta.abs() - TARGET_DELTA).abs()
        calls["dd"] = (calls.delta.abs() - TARGET_DELTA).abs()
        sp = puts.loc[puts.dd.idxmin(), "strike"]
        sc = calls.loc[calls.dd.idxmin(), "strike"]
        lp = _closest_strike(puts.strike, sp - WING_WIDTH)
        lc = _closest_strike(calls.strike, sc + WING_WIDTH)
        legs = [
            {"direction": "short", "type": "put", "strike": sp},
            {"direction": "long", "type": "put", "strike": lp},
            {"direction": "short", "type": "call", "strike": sc},
            {"direction": "long", "type": "call", "strike": lc},
        ]
    elif strategy_type == "vertical_spread":
        if puts.empty:
            logging.error("Cannot build vertical spread: no puts.")
            return
        puts["dd"] = (puts.delta.abs() - TARGET_DELTA).abs()
        sp = puts.loc[puts.dd.idxmin(), "strike"]
        lp = _closest_strike(puts.strike, sp - WING_WIDTH)
        legs = [
            {"direction": "short", "type": "put", "strike": sp},
            {"direction": "long", "type": "put", "strike": lp},
        ]
    else:
        logging.error("Unknown strategy_type %s; aborting.", strategy_type)
        return

    # 4️⃣ Compute entry mid‑prices and net entry price
    opts_df["mid_price"] = (opts_df.bid + opts_df.ask) / 2.0
    price_map = opts_df.set_index(["strike", "option_type"])["mid_price"].to_dict()
    for leg in legs:
        if (leg["strike"], leg["type"]) not in price_map:
            logging.error("Missing mid-price for leg %s; aborting.", leg)
            return
    entry_price = sum(
        price_map[(leg["strike"], leg["type"])] * (1 if leg["direction"] == "long" else -1)
        for leg in legs
    )

    # 5️⃣ Insert into trade_recommendations
    trade_id = f"{strategy_type.upper()}_{now_dt.strftime('%Y%m%d%H%M%S')}"
    rec = {
        "trade_id": trade_id,
        "strategy_type": strategy_type,
        "symbol": symbol,
        "timestamp": timestamp,
        "entry_time": timestamp,
        "expiration_date": expiry,
        "entry_price": entry_price,
        "status": "active",
        "model_version": MODEL_VERSION,
        "notes": f"Auto 0DTE {strategy_type} expires {expiry}",
    }
    CLIENT.insert_rows_json(TRADE_RECS, [rec])

    # 6️⃣ Insert each leg into trade_legs
    leg_rows = []
    for leg in legs:
        lid = f"{trade_id}_{leg['type'].upper()}_{leg['strike']}"
        leg_rows.append(
            {
                "trade_id": trade_id,
                "leg_id": lid,
                "timestamp": timestamp,
                "leg_type": leg["type"],
                "direction": leg["direction"],
                "strike": leg["strike"],
                "entry_price": price_map[(leg["strike"], leg["type"])],
                "status": "open",
                "notes": f"Auto 0DTE {strategy_type} expires {expiry}",
            }
        )
    CLIENT.insert_rows_json(TRADE_LEGS, leg_rows)

    logging.info("✅ Generated %s with legs %s", trade_id, legs)

    # 7️⃣ Trigger P/L analysis, passing both symbol and root_symbol
    legs_df_for_analysis = pd.DataFrame(leg_rows)[
        ["leg_type", "direction", "strike", "entry_price"]
    ]
    compute_and_store_pl_analysis(
        trade_id=trade_id, legs_data=legs_df_for_analysis, spot_price=spot, root_symbol=root_sym
    )
