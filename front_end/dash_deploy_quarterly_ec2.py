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

MONTHS_DICT = {'1st Quarter' : '02',
               '2nd Quarter' : '05',
               '3rd Quarter': '08',
               '4th Quarter' : '11'
                }

NAICS_FILE = "/home/kurlovs/Dropbox/andre/insight/labor_project/NAICS_2017.txt"

YEAR_1 = 2006
YEAR_2 = 2019


def process_date(year, month):
    '''processes date to the format
       found in tables'''
    return(datetime.strptime(f"{MONTHS_DICT[month]}/15/{year}", '%m/%d/%Y'))


def get_industries(naics_file):
    '''processed naics file
       of industry classification
       and outputs dictionary of superindustries
       and subsidiaries'''
    naics_dict = {}

    with open(naics_file) as naics_handle:
        for liney in naics_handle:
            tliney = liney.strip().split("\t")
            category = tliney[0]
            industry = tliney[1].lower()

            if len(category) == 2 or "-" in category:
                naics_dict[industry] = {}
                current_super = industry
                naics_dict[current_super][current_super] = set([current_super])
            elif len(category) == 3:
                current_sub = industry
                naics_dict[current_super][current_sub] = set([current_sub])
            else:
                naics_dict[current_super][current_sub].add(industry)

    return(naics_dict)


PROCESS_NAICS = get_industries(NAICS_FILE)

def date_monthly(year_1, year_2):
    '''gets all the supergroups for monthly data'''
    years = sorted(list(range(year_1, year_2+1)))
    years = years[::-1]
    months = ['1st Quarter',
               '2nd Quarter',
               '3rd Quarter',
               '4th Quarter']
            
    return(years, months)

YEARS_MONTHS = date_monthly(YEAR_1, YEAR_2)

def query_sector_by_area(supersector, subsector, industry,
                            beg_year, beg_month, 
                                end_year, end_month,
                                nplot, typey="total growth", 
                                ): #industry, beg_year, beg_month, end_year, end_mont
    '''plots stuff FROM quarterly database given parameters'''

    real_beg_date = process_date(beg_year, beg_month)
    real_end_date = process_date(end_year, end_month)


    table_1 = sqlio.read_sql(f'''SELECT area,value FROM quarterly
                   WHERE supersector='{supersector}'
                   AND subsector='{subsector}'
                   AND industry='{industry}'
                   AND date='{real_beg_date}'
                   AND area != 'statewide'
                   ORDER BY area
                   ''', con)


    table_2 = sqlio.read_sql(f'''SELECT area,value FROM quarterly
                   WHERE supersector='{supersector}'
                   AND subsector='{subsector}'
                   AND industry='{industry}'
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
        loc_frame_to_plot = sqlio.read_sql(f'''SELECT date,value FROM quarterly
                                WHERE supersector='{supersector}'
                                AND subsector='{subsector}'
                                AND industry='{industry}'
                                AND area='{locale[1]}'
                                AND date BETWEEN '{real_beg_date}' AND '{real_end_date}'
                                ORDER BY date
                                ''', con)


        dates = loc_frame_to_plot["date"]
        sums = loc_frame_to_plot["value"]

        data.append({'x' : list(dates), 'y': list(sums),
                      'type': 'line', 
                      'name': f"#{locale[0]} in growth: {locale[1]}"})
        
    data = {'data': data, 'layout': {
                                     'yaxis': {'title':'Employees (in thousands)'},
                                     'font':  {'size': 18},
                                     'legend': {'y': -1.02},
                                     }     
            }
    
    return(data)


#DASH HERE

app = dash.Dash(__name__)

app = dash.Dash()

app.layout = html.Div(
    [
        html.Div([
        dcc.Dropdown(
            id='name-dropdown',
            options=[{'label':name, 'value':name} for name in sorted(PROCESS_NAICS)],
            placeholder="FIRST SELECT A SUPERSECTOR"
            ),
            ],
            style={'width': '33%', 'display': 'inline-block'},
            
        ),
        
        html.Div([
        dcc.Dropdown(
            id='opt-dropdown',
            placeholder=" THEN CHOOSE A SUBSECTOR"
            ),
            ],
            style={'width': '33%', 'display': 'inline-block'},
            
        ),
    
            html.Div([
        dcc.Dropdown(
            id='opt2-dropdown',
            placeholder='FINALLY, SELECT INDUSTRY'
            ),
            ],
            style={'width': '33%', 'display': 'inline-block'},
        ),

        html.Div([
        dcc.Dropdown(
            id='beg-year',
            options=[{'label':year, 'value':year} for year in YEARS_MONTHS[0]],
            placeholder='START YEAR'
            ),
            ],
            style={'width': '15%', 'display': 'inline-block'},
        ),


        html.Div([
        dcc.Dropdown(
            id='beg-month',
            options=[{'label':month, 'value':month} for month in YEARS_MONTHS[1]],
            placeholder='START QUARTER'
            ),
            ],
            style={'width': '15%', 'display': 'inline-block'},
            
        ),


        html.Div([
        dcc.Dropdown(
            id='end-year',
            options=[{'label':year, 'value':year} for year in YEARS_MONTHS[0]],
            placeholder='END YEAR'
            ),
            ],
            style={'width': '15%', 'display': 'inline-block'},
        ),

        html.Div([
        dcc.Dropdown(
            id='end-month',
            options=[{'label':month, 'value':month} for month in YEARS_MONTHS[1]],
            placeholder='END QUARTER'
            ),
            ],
            style={'width': '15%', 'display': 'inline-block'},
        ),

        html.Div([
        dcc.Dropdown(
            id='how-many',
            placeholder='TOP',
            options=[{'label': i, 'value': i} for i in range(1,11)]
            ),
            ],
            style={'width': '5%', 'display': 'inline-block'},
        ),


        html.Div([
        dcc.Dropdown(
            id='calc-type',
            options=[{'label': i, 'value': i} for i in ["total growth", "percent growth"]],
            placeholder='METRIC'
            ),
            ],
            style={'width': '10%', 'display': 'inline-block'},
        ),

        html.Hr(),
        html.Div(id='display-selected-values'),
        
        html.Div([
        dcc.Graph(
            id='graph')
        ],
        style={'width': '90%' }
        )
    ]
   )


@app.callback(
    dash.dependencies.Output('opt-dropdown', 'options'),
    [dash.dependencies.Input('name-dropdown', 'value')]
)
def update_dropdown(name):
    return [{'label': name2, 'value': name2} for name2 in sorted(PROCESS_NAICS[name])]


@app.callback(
    dash.dependencies.Output('opt2-dropdown', 'options'),
    [dash.dependencies.Input('name-dropdown', 'value'),
    dash.dependencies.Input('opt-dropdown', 'value')]
)
def update_dropdown2(name, name2):
    return [{'label': name3, 'value': name3} for name3 in sorted(PROCESS_NAICS[name][name2])]


@app.callback(
    dash.dependencies.Output('graph', 'figure'),
    [dash.dependencies.Input('name-dropdown', 'value'),
     dash.dependencies.Input('opt-dropdown', 'value'),
     dash.dependencies.Input('opt2-dropdown', 'value'),
     dash.dependencies.Input('beg-year', 'value'),
     dash.dependencies.Input('beg-month', 'value'),
     dash.dependencies.Input('end-year', 'value'),
     dash.dependencies.Input('end-month', 'value'),
     dash.dependencies.Input('how-many', 'value'),
     dash.dependencies.Input('calc-type', 'value')])
def update_figure(supersector, subsector, industry, beg_year, beg_month, 
                  end_year, end_month, howmany, calctype):
    return(query_sector_by_area(supersector, subsector, industry, 
           beg_year, beg_month, end_year, end_month, howmany, calctype))


application = app.server

if __name__ == '__main__':
    application.run(port=8080, server="54.70.154.38"))




