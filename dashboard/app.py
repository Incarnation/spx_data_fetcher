# app.py
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add project root to sys.path so imports like `from common.auth` work
sys.path.append(str(Path(__file__).resolve().parents[1]))


# Load .env only in local development
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    from pathlib import Path

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")


from dash import Dash, Input, Output, dcc, html
from plotly.graph_objects import Bar, Figure
from utils.bq_queries import get_available_expirations, get_gamma_exposure_for_expiry

# Initialize the Dash app
app = Dash(__name__)
app.title = "SPX Gamma Exposure Dashboard"

# Layout: Structured into a heading, dropdown selector, and graph
app.layout = html.Div(
    style={"fontFamily": "Arial", "maxWidth": "1000px", "margin": "auto", "padding": "20px"},
    children=[
        html.H1("📈 SPX Gamma Exposure", style={"textAlign": "center"}),
        html.P(
            "Select an expiration date to view gamma exposure by strike.",
            style={"textAlign": "center"},
        ),
        dcc.Dropdown(
            id="expiration-dropdown",
            options=[{"label": date, "value": date} for date in get_available_expirations()][::-1],
            placeholder="Select an expiration date",
            style={"marginBottom": "30px"},
        ),
        dcc.Graph(id="gex-chart"),
    ],
)


# Callback to update chart based on dropdown selection
@app.callback(
    Output("gex-chart", "figure"),
    Input("expiration-dropdown", "value"),
)
def update_chart(expiration_date):
    """
    Update gamma exposure bar chart based on selected expiration date.
    Red = negative net GEX, Blue = positive.
    """
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
                "title": f"No data available for {expiration_date}",
                "xaxis": {"title": "Strike Price"},
                "yaxis": {"title": "Net Gamma Exposure"},
            }
        }

    # Split positive and negative bars for color distinction
    pos_df = df[df["net_gamma_exposure"] >= 0]
    neg_df = df[df["net_gamma_exposure"] < 0]

    fig = Figure()

    fig.add_trace(
        Bar(
            x=pos_df["strike"],
            y=pos_df["net_gamma_exposure"],
            name="Positive GEX",
            marker_color="blue",
            hovertemplate="Strike: %{x}<br>GEX: %{y:,.0f}<extra></extra>",
        )
    )

    fig.add_trace(
        Bar(
            x=neg_df["strike"],
            y=neg_df["net_gamma_exposure"],
            name="Negative GEX",
            marker_color="red",
            hovertemplate="Strike: %{x}<br>GEX: %{y:,.0f}<extra></extra>",
        )
    )

    # Spot price line
    fig.add_shape(
        type="line",
        x0=spot_price,
        x1=spot_price,
        y0=df["net_gamma_exposure"].min(),
        y1=df["net_gamma_exposure"].max(),
        line=dict(color="black", width=2, dash="dash"),
    )

    # Baseline at GEX = 0
    fig.add_shape(
        type="line",
        x0=df["strike"].min(),
        x1=df["strike"].max(),
        y0=0,
        y1=0,
        line=dict(color="gray", width=1, dash="dot"),
    )

    fig.update_layout(
        title=f"Gamma Exposure on {expiration_date} | Spot ≈ {spot_price:.2f}",
        xaxis_title="Strike Price",
        yaxis_title="Net Gamma Exposure (Contracts × Gamma × 100)",
        yaxis_tickformat=",",
        bargap=0.05,
        template="plotly_white",
        height=500,
    )

    return fig


# Run the app locally
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
