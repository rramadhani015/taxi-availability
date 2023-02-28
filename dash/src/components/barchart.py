from dash import Dash, html, dcc
from dash.dependencies import Input,Output
from ..data.loader import DataSchema
import plotly.express as px
import pandas as pd
from . import ids

# DATA= px.data.medals_long()

def render(app: Dash, data: pd.DataFrame) -> html.Div:
    @app.callback(
        Output(ids.BAR_CHART, "children"),
        Input(ids.CATEGORY_DROPDOWN, "value")
    )
    def update_barchart(category:list[str]) -> html.Div:
        filtered_data = data.query("category in @category")

        # dfs=data.groupby('category').sum()
        dfmean=filtered_data.groupby('category').mean()

        if filtered_data.shape[0] == 0:
            return html.Div("No data selected.")
        
        def create_pivot_table() -> pd.DataFrame:
            pt = filtered_data.pivot_table(
                # values=dfmean.index,
                columns=dfmean['average_rating'],
                aggfunc="mean",
                fill_value=0
            )
            return pt.reset_index()

        fig = px.bar(
            create_pivot_table(),
            x=dfmean.index,
            y=dfmean['average_rating'],
            color=dfmean.index,
            text=dfmean['average_rating'],
            labels={
                     "x": "Category",
                     "y": "Average Rating",
                     "color":"Legends"
                 },
            title="Average Rating"
        )
        return html.Div(dcc.Graph(figure=fig), id=ids.BAR_CHART)
        # fig = px.bar(filtered_data, x="medal", y="count", color="nation", text="nation")
        
    return html.Div(id=ids.BAR_CHART)