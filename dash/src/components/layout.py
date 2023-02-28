import requests
import ast
import json
from dash import Dash, html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
from . import barchart, selections_dropdown
import dash_leaflet as dl
# import dash_leaflet.express as dlx
import matplotlib.pyplot as plt
import geopandas as gpd
from folium.features import GeoJsonPopup, GeoJsonTooltip
# import contextily as cx
import folium
import mapclassify
from shapely.geometry import Point, Polygon
import psycopg2
from datetime import datetime, timedelta


db1 = psycopg2.connect(
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432",
    database="airflow")


def get_api():
    response = requests.get(
        'https://api.data.gov.sg/v1/transport/taxi-availability')
    data = response.json()
    return data


def pg_to_df(query, conn):
    try:
        return pd.read_sql(query, conn)
    except Exception as error:
        raise ("Error fetching data from PostgreSQL:", error)


def create_layout_old(app: Dash, data: pd.DataFrame) -> html.Div:
    return html.Div(
        className="app-div",
        children=[
            html.H1(app.title),
            html.Hr(),
            # html.Div(className="dropdown-container",
            # children=[selections_dropdown.render(app)]),
            html.Div(
                className="dropdown-container",
                children=[
                    selections_dropdown.render_category(app, data)]
            ),
            barchart.render(app, data)
        ]
    )


def timeseries():
    return f"""select TO_CHAR(properties_timestamp, 'YYYY-MM-DD HH24')as hourly,avg(properties_taxi_count)::INT as average_total_taxi from db1.taxi_availability ta
        group by 1 order by 1"""


def last_hour():
    # last_hour_date_time = datetime.now() - timedelta(hours = 1)
    # print(last_hour_date_time.strftime('%Y-%m-%d %H:%M:%S'))
    return f"""with data2 as(
        with data1 as(
        select TO_CHAR(properties_timestamp, 'YYYY-MM-DD HH24')as hourly,geometry_coordinates from db1.taxi_availability ta
        where properties_timestamp >= now() - interval '1 hour' group by 1,2
        )
        SELECT hourly,replace(replace(coordinates, '[[', ''), ']]', '') as coordinates
        FROM   data1, unnest(string_to_array(geometry_coordinates, '], [')) coordinates
        )
        select hourly,split_part(coordinates, ', ', 1) as lat,split_part(coordinates, ', ', 2) as long, count(*) as total from data2
        group by 1,coordinates"""


def last_5rows():
    return f"""with data2 as(
        with data1 as(
        select TO_CHAR(properties_timestamp, 'YYYY-MM-DD HH24')as hourly,geometry_coordinates from db1.taxi_availability ta
        limit 5
        )
        SELECT hourly,replace(replace(coordinates, '[[', ''), ']]', '') as coordinates
        FROM   data1, unnest(string_to_array(geometry_coordinates, '], [')) coordinates
        )
        select hourly,split_part(coordinates, ', ', 1) as lat,split_part(coordinates, ', ', 2) as long, count(*) as total from data2
        group by 1,coordinates"""


def generate_data(query):
    df_geo = pg_to_df(query, db1)
    gdf_taxi = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(
        df_geo['lat'], df_geo['long']), crs='4326')
    # geojson = json.loads(gdf_taxi.to_json())
    # return geojson

    # gdf_taxi = gdf.to_crs('EPSG:4326')
    area = 'Subzone_Census2010'
    gdf_area = gpd.read_file("./src/data/shp/"+area+".shp")
    gdf_area = gdf_area.to_crs(epsg=4326)

    # Spatial Joins
    pointsInPolygon = gpd.sjoin(
        gdf_taxi, gdf_area, how="inner", op='intersects')

    # Add a field with 1 as a constant value
    pointsInPolygon['value'] = 1

    # Group according to the column by which you want to aggregate data
    sum_df = pointsInPolygon.groupby(['OBJECTID']).sum()
    # gdf_area
    df_area_sum = gdf_area.merge(sum_df, on='OBJECTID')

    gdf_area_sum = gpd.GeoDataFrame(
        df_area_sum, geometry=df_area_sum['geometry'], crs='4326')

    m = gdf_area_sum.explore(
        tooltip={"OBJECTID", "SUBZONE_N", "value"},
        column="SUBZONE_N",  # make choropleth based on "BoroName" column
        scheme="naturalbreaks",  # use mapclassify's natural breaks scheme
        legend=True,  # show legend
        k=10,  # use 10 bins
        legend_kwds=dict(colorbar=False),  # do not use colorbar
        name="subzone"  # name of the layer in the map
    )

    gdf_taxi.explore(
        m=m,  # pass the map object
        color="blue",  # use red color on all points
        # make marker radius 10px with fill
        marker_kwds=dict(radius=3, fill=True),
        #  tooltip="name", # show "name" column in the tooltip
        # do not show column label in the tooltip
        tooltip_kwds=dict(labels=False),
        name="taxi"  # name of the layer in the map
    )

    # m = gdf_taxi.explore(
    #     # tooltip = {"OBJECTID","SUBZONE_N", "const"},
    #     column="total",  # make choropleth based on "BoroName" column
    #     scheme="naturalbreaks",  # use mapclassify's natural breaks scheme
    #     legend=True, # show legend
    #     k=5, # use 10 bins
    #     legend_kwds=dict(colorbar=False), # do not use colorbar
    #     name="subzone" # name of the layer in the map
    # )

    folium.TileLayer('Stamen Toner', control=True).add_to(
        m)  # use folium to add alternative tiles
    folium.LayerControl().add_to(m)  # use folium to add layer control

    html_string = m.get_root().render()
    return html_string  # showmap


def generate_data_top10(query):
    df_geo = pg_to_df(query, db1)
    gdf_taxi = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(
        df_geo['lat'], df_geo['long']), crs='4326')
    # geojson = json.loads(gdf_taxi.to_json())
    # return geojson

    # gdf_taxi = gdf.to_crs('EPSG:4326')
    area = 'Subzone_Census2010'
    gdf_area = gpd.read_file("./src/data/shp/"+area+".shp")
    gdf_area = gdf_area.to_crs(epsg=4326)

    # Spatial Joins
    pointsInPolygon = gpd.sjoin(
        gdf_taxi, gdf_area, how="inner", op='intersects')

    # Add a field with 1 as a constant value
    pointsInPolygon['value'] = 1

    # Group according to the column by which you want to aggregate data
    sum_df = pointsInPolygon.groupby(['OBJECTID']).sum()
    # gdf_area
    df_area_sum = gdf_area.merge(sum_df, on='OBJECTID')

    # top10
    top10 = df_area_sum.nlargest(10, 'value')
    top10.rename(columns={'index_left': 'left',
                 'index_right': 'right'}, inplace=True)
    # top10.drop(['index_left', 'index_right'], axis=1)
    gdf_area_sum = gpd.GeoDataFrame(
        top10, geometry=top10['geometry'], crs='4326')

    # Spatial Joins
    points_top10 = gpd.sjoin(gdf_taxi, gdf_area_sum,
                             how="inner", op='intersects')
    gdf_taxi = gpd.GeoDataFrame(
        points_top10, geometry=points_top10['geometry'], crs='4326')

    m = gdf_area_sum.explore(
        tooltip={"OBJECTID", "SUBZONE_N", "value"},
        column="SUBZONE_N",  # make choropleth based on "BoroName" column
        scheme="naturalbreaks",  # use mapclassify's natural breaks scheme
        legend=True,  # show legend
        k=10,  # use 10 bins
        legend_kwds=dict(colorbar=False),  # do not use colorbar
        name="subzone"  # name of the layer in the map
    )

    gdf_taxi.explore(
        m=m,  # pass the map object
        color="blue",  # use red color on all points
        # make marker radius 10px with fill
        marker_kwds=dict(radius=3, fill=True),
        #  tooltip="name", # show "name" column in the tooltip
        # do not show column label in the tooltip
        tooltip_kwds=dict(labels=False),
        name="taxi"  # name of the layer in the map
    )

    folium.TileLayer('Stamen Toner', control=True).add_to(
        m)  # use folium to add alternative tiles
    folium.LayerControl().add_to(m)  # use folium to add layer control

    html_string = m.get_root().render()
    return html_string  # showmap


def generate_data_bot10(query):
    df_geo = pg_to_df(query, db1)
    gdf_taxi = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(
        df_geo['lat'], df_geo['long']), crs='4326')
    # geojson = json.loads(gdf_taxi.to_json())
    # return geojson

    # gdf_taxi = gdf.to_crs('EPSG:4326')
    area = 'Subzone_Census2010'
    gdf_area = gpd.read_file("./src/data/shp/"+area+".shp")
    gdf_area = gdf_area.to_crs(epsg=4326)

    # Spatial Joins
    pointsInPolygon = gpd.sjoin(
        gdf_taxi, gdf_area, how="inner", op='intersects')

    # Add a field with 1 as a constant value
    pointsInPolygon['value'] = 1

    # Group according to the column by which you want to aggregate data
    sum_df = pointsInPolygon.groupby(['OBJECTID']).sum()
    # gdf_area
    df_area_sum = gdf_area.merge(sum_df, on='OBJECTID')

    # low10
    bot10 = df_area_sum.nsmallest(10, 'value')
    bot10.rename(columns={'index_left': 'left',
                 'index_right': 'right'}, inplace=True)
    # top10.drop(['index_left', 'index_right'], axis=1)
    gdf_area_sum = gpd.GeoDataFrame(
        bot10, geometry=bot10['geometry'], crs='4326')

    # Spatial Joins
    points_bot10 = gpd.sjoin(gdf_taxi, gdf_area_sum,
                             how="inner", op='intersects')
    gdf_taxi = gpd.GeoDataFrame(
        points_bot10, geometry=points_bot10['geometry'], crs='4326')

    m = gdf_area_sum.explore(
        tooltip={"OBJECTID", "SUBZONE_N", "value"},
        column="SUBZONE_N",  # make choropleth based on "BoroName" column
        scheme="naturalbreaks",  # use mapclassify's natural breaks scheme
        legend=True,  # show legend
        k=10,  # use 10 bins
        legend_kwds=dict(colorbar=False),  # do not use colorbar
        name="subzone"  # name of the layer in the map
    )

    gdf_taxi.explore(
        m=m,  # pass the map object
        color="blue",  # use red color on all points
        # make marker radius 10px with fill
        marker_kwds=dict(radius=3, fill=True),
        #  tooltip="name", # show "name" column in the tooltip
        # do not show column label in the tooltip
        tooltip_kwds=dict(labels=False),
        name="taxi"  # name of the layer in the map
    )

    folium.TileLayer('Stamen Toner', control=True).add_to(
        m)  # use folium to add alternative tiles
    folium.LayerControl().add_to(m)  # use folium to add layer control

    html_string = m.get_root().render()
    return html_string  # showmap


def generate_data_nointersection(query):
    df_geo = pg_to_df(query, db1)
    gdf_taxi = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(
        df_geo['lat'], df_geo['long']), crs='4326')
    # geojson = json.loads(gdf_taxi.to_json())
    # return geojson

    # gdf_taxi = gdf.to_crs('EPSG:4326')
    area = 'Subzone_Census2010'
    gdf_area = gpd.read_file("./src/data/shp/"+area+".shp")
    gdf_area = gdf_area.to_crs(epsg=4326)

    # Spatial Joins
    pointsInPolygon = gpd.sjoin(
        gdf_taxi, gdf_area, how="inner", op='intersects')

    # Add a field with 1 as a constant value
    pointsInPolygon['value'] = 1

    # Group according to the column by which you want to aggregate data
    sum_df = pointsInPolygon.groupby(['OBJECTID']).sum()
    df_area_sum = gdf_area.merge(sum_df, on='OBJECTID')

    df_area = pd.DataFrame(gdf_area)
    df_all = df_area.merge(df_area_sum.drop_duplicates(), on=['OBJECTID'],
                           how='left', indicator=True)
    df_notin = df_all[df_all['_merge'] == 'left_only']

    gdf_area_sum = gpd.GeoDataFrame(
        df_notin, geometry=df_notin['geometry_x'], crs='4326')

    m = gdf_area_sum.explore(
        tooltip={"OBJECTID", "SUBZONE_N_x"},
        column="SUBZONE_N_x",  # make choropleth based on "BoroName" column
        scheme="naturalbreaks",  # use mapclassify's natural breaks scheme
        legend=True,  # show legend
        k=10,  # use 10 bins
        legend_kwds=dict(colorbar=False),  # do not use colorbar
        name="subzone"  # name of the layer in the map
    )

    # gdf_taxi.explore(
    #     m=m, # pass the map object
    #     color="blue", # use red color on all points
    #     marker_kwds=dict(radius=3, fill=True), # make marker radius 10px with fill
    #     #  tooltip="name", # show "name" column in the tooltip
    #     tooltip_kwds=dict(labels=False), # do not show column label in the tooltip
    #     name="taxi" # name of the layer in the map
    # )

    folium.TileLayer('Stamen Toner', control=True).add_to(
        m)  # use folium to add alternative tiles
    folium.LayerControl().add_to(m)  # use folium to add layer control

    html_string = m.get_root().render()
    return html_string  # showmap


all_options = {
    'Last 5 Rows': ['Last 5 Rows'],
    'Last Hour': ['Last Hour']
}


def create_layout(app: Dash) -> html.Div:
    @app.callback([Output('display-query', 'children'), Output('display-1', 'srcDoc'), Output('display-2', 'srcDoc'), Output('display-3', 'srcDoc'), Output('display-4', 'srcDoc')],
                  [Input('query-radio', 'value')])
    def set_query(selected_query):
        if selected_query == 'Last 5 Rows':
            query = last_5rows()
        if selected_query == 'Last Hour':
            query = last_hour()

        html_string_all = generate_data(query)
        html_string_top10 = generate_data_top10(query)
        html_string_bot10 = generate_data_bot10(query)
        html_string_nointersection = generate_data_nointersection(query)
        return 'Display '+selected_query, html_string_all, html_string_top10, html_string_bot10, html_string_nointersection
    
    df_ts=pg_to_df(timeseries(), db1)
    graph_data = []
    graph_data.append({'x': df_ts['hourly'], 'y': df_ts['average_total_taxi']})

    return html.Div(
        className="container-fluid",
        children=[
            html.Div(
                className="row",
                children=[
                    html.Div(
                        className="col"
                    ),
                    html.Div(
                        className="col-6",
                        children=[
                            html.Div(
                                className="app-div",
                                children=[
                                    html.H1(app.title),
                                    html.Hr()
                                ]
                            )
                        ]
                    ),
                    html.Div(
                        className="col"
                    )
                ]
            ),
            html.Div(
                className="row",
                children=[
                    html.Div(
                        className="col"
                    ),
                    html.Div(
                        className="col-6",
                        children=[
                            html.Div(
                                className="app",
                                children=[
                                    html.Div(
                                        dbc.Row(
                                            [
                                                dbc.Col(dcc.Graph(id="average_taxi_availability", figure={
                                                    'data': graph_data,
                                                    'layout': {'title':'Average Taxi Availability',
                                                        # Same x and first y
                                                        'xaxis':dict(title='Date and time'),
                                                        'yaxis':dict(title='Average Taxi Available')}
                                                }),
                                                    width={"size": 8, "offset": 2}),
                                            ]
                                        )),
                                    html.Hr()
                                ]
                            )
                        ]
                    ),
                    html.Div(
                        className="col"
                    )
                ]
            ),
            html.Div(
                className="row",
                children=[
                    html.Div(
                        className="col"
                    ),
                    html.Div(
                        className="col-6",
                        children=[
                            html.Div(
                                className="app",
                                children=[
                                    html.H4(
                                        "Subzonal taxi availabilty"),
                                    dbc.RadioItems(
                                        list(all_options.keys()),
                                        'Last 5 Rows',
                                        id='query-radio',
                                    ),
                                    html.Div(id='display-query'),
                                    html.Hr()
                                ]
                            )
                        ]
                    ),
                    html.Div(
                        className="col"
                    )
                ]
            ),
            html.Div(
                className="row",
                children=[
                    html.Div(
                        className="col"
                    ),
                    html.Div(
                        className="col-6",
                        children=[
                            html.Div(
                                className="app",
                                children=[
                                    html.H5(
                                        "All subzones with taxi available"),
                                    html.Div([
                                        # html.Iframe(srcDoc=html_string_all,d='display-query',
                                        html.Iframe(id='display-1',
                                                    style={"height": "500px", "width": "100%"})
                                    ]),
                                    html.Hr()
                                ]
                            )
                        ]
                    ),
                    html.Div(
                        className="col"
                    )
                ]
            ),
            html.Div(
                className="row",
                children=[
                    html.Div(
                        className="col"
                    ),
                    html.Div(
                        className="col-6",
                        children=[
                            html.Div(
                                className="app",
                                children=[
                                    html.H5(
                                        "Top 10 subzones with taxi available"),
                                    html.Div([
                                        # html.Iframe(srcDoc=html_string_top10,
                                        html.Iframe(id='display-2',
                                                    style={"height": "500px", "width": "100%"})
                                    ]),
                                    html.Hr()
                                ]
                            )
                        ]
                    ),
                    html.Div(
                        className="col"
                    )
                ]
            ),
            html.Div(
                className="row",
                children=[
                    html.Div(
                        className="col"
                    ),
                    html.Div(
                        className="col-6",
                        children=[
                            html.Div(
                                className="app",
                                children=[
                                    html.H5(
                                        "Bottom 10 subzones with taxi available"),
                                    html.Div([
                                        # html.Iframe(srcDoc=html_string_bot10,
                                        html.Iframe(id='display-3',
                                                    style={"height": "500px", "width": "100%"})
                                    ]),
                                    html.Hr()
                                ]
                            )
                        ]
                    ),
                    html.Div(
                        className="col"
                    )
                ]
            ),
            html.Div(
                className="row",
                children=[
                    html.Div(
                        className="col"
                    ),
                    html.Div(
                        className="col-6",
                        children=[
                            html.Div(
                                className="app",
                                children=[
                                    html.H5(
                                        "All subzones with no taxi available"),
                                    html.Div([
                                        # html.Iframe(srcDoc=html_string_nointersection,
                                        html.Iframe(id='display-4',
                                                    style={"height": "500px", "width": "100%"})
                                    ]),
                                    html.Hr()
                                ]
                            )
                        ]
                    ),
                    html.Div(
                        className="col"
                    )
                ]
            )
        ]
    )
