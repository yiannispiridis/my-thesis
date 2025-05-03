import dash
from dash import dcc, html, dash_table
import plotly.express as px
from dash.dependencies import Input, Output
import asyncio

from backend.database.connection import db
from backend.database.repository import get_stock_data_df, get_normalized_data_df


async def main():
    await db.init_pool()

    stock_data_df = await get_stock_data_df(db.pool)
    normalized_stock_data_df = await get_normalized_data_df(db.pool)

    await asyncio.sleep(10)
    app = dash.Dash(__name__)
    app.title = "Stock Clustering Dashboard"

    app.layout = html.Div([
        html.H1("Stock Clustering Dashboard"),
        dcc.Tabs(id='tabs', value='scatter', children=[
            dcc.Tab(label='Scatter Plot', value='scatter'),
            dcc.Tab(label='Data Table raw', value='table_raw'),
            dcc.Tab(label='Data Table normalized', value='table_normalized'),
            dcc.Tab(label='Cluster Analysis', value='cluster'),
        ]),
        html.Div(id='tabs-content')
    ])

    @app.callback(
        Output('tabs-content', 'children'),
        [Input('tabs', 'value')]
    )
    def render_tab_content(tab_value):
        if tab_value == 'scatter':
            return html.Div([
                html.H2("Data figure")
            ])
        elif tab_value == 'table_raw':
            return html.Div([
                html.H2("Raw Data"),
                dash_table.DataTable(
                    columns=[{"name": col, "id": col} for col in stock_data_df.columns],
                    data=stock_data_df.to_dict("records"),
                    page_size=15,
                    sort_action="native",
                    filter_action="native",
                    virtualization=True,
                    style_table={"overflowX": "auto"},
                    style_cell={"padding": "5px", "textAlign": "left"},
                )
            ])
        elif tab_value == 'table_normalized':
            return html.Div([
                html.H2("Normalized Data"),
                dash_table.DataTable(
                    columns=[{"name": col, "id": col} for col in normalized_stock_data_df.columns],
                    data=normalized_stock_data_df.to_dict("records"),
                    page_size=15,
                    sort_action="native",
                    filter_action="native",
                    virtualization=True,
                    style_table={"overflowX": "auto"},
                    style_cell={"padding": "5px", "textAlign": "left"},
                )
            ])
        elif tab_value == 'cluster':
            return html.Div([
                html.H2("Cluster Analysis"),
                html.P("Here you could display cluster analysis results or another visualization.")
            ])

    app.run(debug=True)

if __name__ == "__main__":
    asyncio.run(main())

