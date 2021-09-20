from os.path import isfile, join
from os import listdir
import pandas as pd
import plotly.express as px  # (version 4.7.0)
import plotly.graph_objects as go

import dash  # (version 1.12.0) pip install dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import base64
import datetime
import io
import dash_table
from pyomo.environ import *
from optimization import TransitionModel
import json
# from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing

import importlib


# importlib.import_module("./optimization.py", package='run_all')

external_stylesheets = [

]
app = dash.Dash(__name__)

# ---------- Import and clean data (importing csv into pandas)
# df = pd.read_csv("intro_bees.csv")

onlyfiles = [f for f in listdir("preprocessing/raw_data/demand/local_demand_timeseries")
             if isfile(join("preprocessing/raw_data/demand/local_demand_timeseries", f))]

options_arr = [


]


for file in onlyfiles:
    options_dict = {}
    options_dict["label"] = file.replace(".csv", "")
    options_dict["value"] = file

    options_arr.append(options_dict)

# fig.savefig("output.png")


#
# # ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H1("Visualise demand data over regions",
            style={'text-align': 'center'}),
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },),
    html.Pre(id="logs", children=[]),

    dcc.Dropdown(id="place",
                 options=options_arr,
                 multi=False,
                 value="Dorset.csv",
                 style={'width': "40%"}
                 ),
    # html.Div(id='output_container', children=[]),

    # html.Br(),
    html.Div(id='output-data-upload'),


    dcc.Graph(id='supplypv_ts', figure={}),

    dcc.Graph(id='demand_ts', figure={})

])


def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded), sheet_name=None)
    except Exception as e:
        print(e)
        return None

    return df

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
# pool = ThreadPool(4)


def worker(q, list_of_contents, list_of_names):

    # df = pd.read_excel("processing/input/b.xlsx", sheet_name=None)
    df = parse_contents(list_of_contents, list_of_names)
    print(df)
    
    df["Demand"].columns=df["Demand"].columns.map(lambda x: x.replace(" (MWh)",""))
    df["SupIm"].columns=df["SupIm"].columns.map(lambda x: x.replace(" (m/s)","").replace(" (kJ/m2)",""))
    model = TransitionModel(df)

    model, results = model.optimize()
    # df=pd.read_excel("./processing/input/b.xlsx",sheet_name=None)
    # model=run_all(df)

    # model.display()
    # opt = SolverFactory("ipopt")
    # results = opt.solve(model)
    # return_dict[procnum]=[model,results]
    q.put([model, results])
    # return results


q = multiprocessing.Queue()


@app.callback(

    Output(component_id="logs", component_property='children'),

    [Input(component_id='upload-data', component_property='contents'),
     State('upload-data', 'filename')]
)
def run_optimization(list_of_contents, list_of_names):

    if list_of_contents is not None:

        p = multiprocessing.Process(target=worker, args=(
            q, list_of_contents, list_of_names))

        p.start()

        results = q.get()

        model_dict = {}
        for v in results[0].component_data_objects(Var):
            model_dict[str(v)] = str(v.value)
        # return str(results[0])
        return json.dumps(model_dict, sort_keys=True, indent=4)

    else:
        return "Please select a excel file to begin optimization"

@app.callback(
    [Output(component_id='supplypv_ts', component_property='figure'), Output(
        component_id='demand_ts', component_property='figure'),Output('output-data-upload', 'children')],
    [Input(component_id='place', component_property='value'), Input(component_id='upload-data', component_property='contents'), State('upload-data', 'filename'),
    ]
)
def update_graph(filename, list_of_contents, list_of_names):  # all the inputs


    print("1")
    df=pd.read_excel("./processing/input/b.xlsx",sheet_name=None)
    model=run_all(df)
    print("2",df)
    model.display()
    opt = SolverFactory("ipopt")
    results = opt.solve(model,tee=True)


    print("3")

    print(results)
    children=html.Div(id='fdsfs')
    if list_of_contents is not None:
        # children = [

        #     parse_contents(c, n, d) for c, n, d in
        #     zip(list_of_contents, list_of_names, list_of_dates)]
        # df=parse_contents(list_of_contents, list_of_names)
        print("1")
        df=pd.read_excel("./processing/input/b.xlsx")
        model=run_all(df)
        print("2",df)
        model.display()
        opt = SolverFactory("ipopt")
        results = opt.solve(model,tee=True)


        print("3")

        print(results)
        children=dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns]
        )

    df_demand = pd.read_csv(
        "preprocessing/raw_data/demand/local_demand_timeseries/"+filename, index_col=0)

    fig_demand = px.area(df_demand, y="demand")
    df_pvsupply = pd.read_csv(
        "preprocessing/raw_data/supply/pv/final_output1.csv", index_col=0)
    # x-axis formatter
    fig_pvsupply = px.area(df_pvsupply, y=filename.replace(".csv", ""))

    return fig_pvsupply, fig_demand ,children


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True)


# https://youtu.be/hSPmj7mK6ng
