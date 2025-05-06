# =====================
# dashboard/main.py
# Dash app for SPX Gamma Exposure Visualization
# =====================
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Enable absolute imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Load .env in dev
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

from dash import Dash, Input, Output, callback, dcc, html
from flask_caching import Cache
from plotly.graph_objects import Bar, Figure, Surface
from utils.bq_queries import (
    get_available_expirations,
    get_gamma_exposure_for_expiry,
    get_gamma_exposure_surface_data,
)

# Init Dash app
app = Dash(__name__)
app.title = "📊 SPX Gamma Exposure Dashboard"
cache = Cache(app.server, config={"CACHE_TYPE": "SimpleCache"})


@cache.memoize(timeout=300)
def get_cached_expirations():
    return get_available_expirations()[::-1]


# Layout
app.layout = html.Div(
    style={"fontFamily": "Arial", "maxWidth": "1100px", "margin": "auto", "padding": "20px"},
    children=[
        html.H1("📈 SPX Gamma Exposure", style={"textAlign": "center"}),
        html.P(
            "Select expiration date to view gamma exposure by strike.",
            style={"textAlign": "center"},
        ),
        dcc.Dropdown(
            id="expiration-dropdown",
            options=[{"label": date, "value": date} for date in get_cached_expirations()],
            placeholder="Select expiration date",
            clearable=False,
            style={"marginBottom": "30px"},
        ),
        dcc.Loading(dcc.Graph(id="gex-chart")),
        html.Hr(),
        html.H3("🎯 Gamma Exposure Surface (Strike × Expiry × GEX)", style={"textAlign": "center"}),
        dcc.Loading(dcc.Graph(id="gex-surface", figure=get_gamma_exposure_surface_data())),
    ],
)


@callback(Output("gex-chart", "figure"), Input("expiration-dropdown", "value"))
def update_gamma_chart(expiration_date):
    if not expiration_date:
        return {
            "layout": {
                "title": "Please select an expiration date",
                "xaxis": {"title": "Strike Price"},
                "yaxis": {"title": "Net Gamma Exposure"},
            }
        }

    df, spot_price = get_gamma_exposure_for_expiry(expiration_date)
    if df.empty:
        return {
            "layout": {
                "title": f"No data for {expiration_date}",
                "xaxis": {"title": "Strike Price"},
                "yaxis": {"title": "Net Gamma Exposure"},
            }
        }

    fig = Figure()
    fig.add_trace(
        Bar(
            x=df[df["net_gamma_exposure"] >= 0]["strike"],
            y=df[df["net_gamma_exposure"] >= 0]["net_gamma_exposure"],
            name="Positive GEX",
            marker_color="blue",
        )
    )
    fig.add_trace(
        Bar(
            x=df[df["net_gamma_exposure"] < 0]["strike"],
            y=df[df["net_gamma_exposure"] < 0]["net_gamma_exposure"],
            name="Negative GEX",
            marker_color="red",
        )
    )
    fig.add_shape(
        type="line",
        x0=spot_price,
        x1=spot_price,
        y0=df["net_gamma_exposure"].min(),
        y1=df["net_gamma_exposure"].max(),
        line=dict(color="black", dash="dash"),
    )
    fig.add_shape(
        type="line",
        x0=df["strike"].min(),
        x1=df["strike"].max(),
        y0=0,
        y1=0,
        line=dict(color="gray", dash="dot"),
    )
    fig.update_layout(
        title=f"GEX on {expiration_date} | Spot ≈ {spot_price:.2f}",
        xaxis_title="Strike Price",
        yaxis_title="Net Gamma Exposure (Contracts × Gamma × 100)",
        yaxis_tickformat=",",
        template="plotly_white",
        height=500,
    )
    return fig


if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
