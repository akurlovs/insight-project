import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import psycopg2
import pandas
import pandas.io.sql as sqlio
from datetime import datetime
from math import log

# START DASH

con = psycopg2.connect("dbname='postgres' \
                        user='postgres' \
                        host='postgres.cm6tnfe8mefq.us-west-2.rds.amazonaws.com' \
                        password='SterlingArcher69'")

MONTHS_DICT = {'Jan' : '01',
               'Feb' : '02',
               'Mar' : '03',
               'Apr' : '04',
               'May' : '05',
               'Jun' : '06',
               'Jul' : '07',
               'Aug' : '08',
               'Sep' : '09',
               'Oct' : '10',
               'Nov' : '11',
               'Dec' : '12'}


def process_date(year, month):
    '''processes date to the format
       found in tables'''
    return(datetime.strptime(f"{MONTHS_DICT[month]}/15/{year}", '%m/%d/%Y'))


def get_supersectors():
    '''gets all the supergroups for monthly data'''
    supersectors = sqlio.read_sql(f'''SELECT DISTINCT supersector
                                 FROM monthly''', con)
    return(list(supersectors['supersector']))


def get_industries(supersectors):
    supersectory_ind = {}
    for supersector in supersectors:
        industries = sqlio.read_sql(f'''SELECT DISTINCT industry
                                   FROM monthly
                                  WHERE supersector='{supersector}'
                                 ''', con)
        supersectory_ind[supersector] = list(industries['industry'])
    return(supersectory_ind)

        
def date_monthly():
    '''gets all the supergroups for monthly data'''
    dates = sqlio.read_sql(f'''SELECT DISTINCT date
                                 FROM monthly''', con)
    dates_range = list(dates["date"])
    years = sorted(list(set([i.year for i in dates_range])))
    years = years[::-1]
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May',
              'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 
               'Nov', 'Dec']
    return(years, months)



def query_sector_by_area(supersector, industry,
                            beg_year, beg_month, 
                                end_year, end_month,
                                nplot, typey="total growth"): #industry, beg_year, beg_month, end_year, end_mont
    '''plots stuff from monthly database given parameters'''

    real_beg_date = process_date(beg_year, beg_month)
    real_end_date = process_date(end_year, end_month)

    table_1 = sqlio.read_sql(f'''SELECT area,value FROM monthly
                   WHERE supersector='{supersector}'
                   AND industry='{industry}'
                   AND datatype='all employees, in thousands'
                   AND date='{real_beg_date}'
                   AND area != 'statewide'
                   ORDER BY area
                   ''', con)

    table_2 = sqlio.read_sql(f'''SELECT area,value FROM monthly
                   WHERE supersector='{supersector}'
                   AND industry='{industry}'
                   AND datatype='all employees, in thousands'
                   AND date='{real_end_date}'
                   AND area != 'statewide'
                   ORDER BY area
                   ''', con)


    table_3 = table_2.merge(table_1, on='area')
    if typey == "total growth":
        table_3["difference"] = table_3["value_x"] - table_3["value_y"]
    else:
        table_3["difference"] = (table_3["value_x"] - table_3["value_y"])/table_3["value_x"]
    table_3.sort_values(by="difference", ascending=False, inplace=True)

    #print(table_3.head(n=5))
    tops = table_3.head(n=nplot)["area"]

    data = []

    for locale in zip(range(1,len(tops)+1), tops):
        loc_frame_to_plot = sqlio.read_sql(f'''SELECT date,value FROM monthly
                                WHERE supersector='{supersector}'
                                AND industry='{industry}'
                                AND datatype='all employees, in thousands'
                                AND area='{locale[1]}'
                                AND date BETWEEN '{real_beg_date}' AND '{real_end_date}'
                                ORDER BY date
                                ''', con)

        dates = loc_frame_to_plot["date"]
        sums = loc_frame_to_plot["value"]

        #print(dates)
        #print(sums)

        data.append({'x' : list(dates), 'y': list(sums),
                      'type': 'line', 
                      'name': f"#{locale[0]} in growth: {locale[1]}"})
        
    data = {'data': data, 'layout': {
                                     'yaxis': {'title':'Employees (in thousands)'}, 
                                     'legend':  {'fontsize': 25, 'x':1.02} 
                                     }     
            }
    
    return(data)


supersectors = get_supersectors()
industry_dict = get_industries(supersectors)

years_months = date_monthly()

#DASH HERE

app = dash.Dash(__name__)

app = dash.Dash()

app.layout = html.Div(
    [
        html.Div([
        dcc.Dropdown(
            id='name-dropdown',
            options=[{'label':name, 'value':name} for name in industry_dict],
            value = list(industry_dict.keys())[0]),
            ],
            style={'width': '25%', 'display': 'inline-block'}),
        
        html.Div([
        dcc.Dropdown(
            id='opt-dropdown',
            ),
            ],
            style={'width': '25%', 'display': 'inline-block'}
        ),

        html.Div([
        dcc.Dropdown(
            id='beg-year',
            options=[{'label':year, 'value':year} for year in years_months[0]],
            value = years_months[0]),
            ],
            style={'width': '7%', 'display': 'inline-block'}
        ),


        html.Div([
        dcc.Dropdown(
            id='beg-month',
            options=[{'label':month, 'value':month} for month in years_months[1]],
            value = years_months[1]),
            ],
            style={'width': '7%', 'display': 'inline-block'}
        ),


        html.Div([
        dcc.Dropdown(
            id='end-year',
            options=[{'label':year, 'value':year} for year in years_months[0]],
            value = years_months[0]),
            ],
            style={'width': '7%', 'display': 'inline-block'}
        ),

        html.Div([
        dcc.Dropdown(
            id='end-month',
            options=[{'label':month, 'value':month} for month in years_months[1]],
            value = years_months[1]),
            ],
            style={'width': '7%', 'display': 'inline-block'}
        ),

        html.Div([
        dcc.Dropdown(
            id='how-many',
            options=[{'label': i, 'value': i} for i in range(1,11)],
            value = list(range(1,11))),
            ],
            style={'width': '5%', 'display': 'inline-block'}
        ),


        html.Div([
        dcc.Dropdown(
            id='calc-type',
            options=[{'label': i, 'value': i} for i in ["total growth", "percent growth"]],
            value = ["total growth", "percent growth"])
            ],
            style={'width': '10%', 'display': 'inline-block'}
        ),

        html.Hr(),
        html.Div(id='display-selected-values'),
        
        html.Div([
        dcc.Graph(
            id='graph')
        ],
        style={'legend':  {'fontsize': 25, 'x':1.02} }
        )
    ]
   )

@app.callback(
    dash.dependencies.Output('opt-dropdown', 'options'),
    [dash.dependencies.Input('name-dropdown', 'value')]
)
def update_dropdown(name):
    return [{'label': i, 'value': i} for i in sorted(industry_dict[name])]

@app.callback(
    dash.dependencies.Output('graph', 'figure'),
    [dash.dependencies.Input('name-dropdown', 'value'),
     dash.dependencies.Input('opt-dropdown', 'value'),
     dash.dependencies.Input('beg-year', 'value'),
     dash.dependencies.Input('beg-month', 'value'),
     dash.dependencies.Input('end-year', 'value'),
     dash.dependencies.Input('end-month', 'value'),
     dash.dependencies.Input('how-many', 'value'),
     dash.dependencies.Input('calc-type', 'value')])
def update_figure(supersector, industry, beg_year, beg_month, end_year, end_month, howmany, calctype):
    return(query_sector_by_area(supersector, industry, beg_year, beg_month, end_year, end_month, howmany, calctype))

if __name__ == '__main__':
    app.run_server()




