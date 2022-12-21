import datetime
import itertools
import math
import re

import dash
import dash_core_components as dcc
import dash_html_components as html
import nltk
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from flask import Flask

nltk.download('punkt')
nltk.download('stopwords')
import firebase_admin
from firebase_admin import credentials, db, firestore
from nltk.corpus import stopwords
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize
from textblob import TextBlob
import numpy as np
import settings

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = 'Real-Time Twitter Monitor'
app.layout = html.Div(children=[
    html.H2('Real-time Twitter Sentiment Analysis for Brand Improvement and Topic Tracking ', style={
        'textAlign': 'center'
    }),
    html.H4('(Last updated: Nov 10, 2022)', style={
        'textAlign': 'right'
    }),
    

    html.Div(id='live-update-graph'),
    html.Div(id='live-update-graph-bottom'),

    # Author's Words
    html.Div(
        className='row',
        children=[ 
            dcc.Markdown("__Author's Words__: Dive into the industry and get my hands dirty. That's why I start this self-motivated independent project. If you like it, I would appreciate for starring - my project on [GitHub](https://github.com/PJMAM)!"),
        ],style={'width': '35%', 'marginLeft': 70}
    ),
    html.Br(),
    
    # ABOUT ROW
    html.Div(
        className='row',
        children=[
            html.Div(
                className='three columns',
                children=[
                    html.P(
                    'Data extracted from:'
                    ),
                    html.A(
                        'Twitter API',
                        href='https://developer.twitter.com'
                    )                    
                ]
            ),
            html.Div(
                className='three columns',
                children=[
                    html.P(
                    'Code avaliable at:'
                    ),
                    html.A(
                        'GitHub',
                        href='https://github.com/PJMAM'
                    )                    
                ]
            ),
            html.Div(
                className='three columns',
                children=[
                    html.P(
                    'Made with:'
                    ),
                    html.A(
                        'Dash / Plot.ly',
                        href='https://plot.ly/dash/'
                    )                    
                ]
            ),
            html.Div(
                className='three columns',
                children=[
                    html.P(
                    'Author:'
                    ),
                    html.A(
                        'PJ Mam',
                        href='https://www.linkedin.com/in/phuoc-nguyen-b662891bb/'
                    )                    
                ]
            )                                                          
        ], style={'marginLeft': 70, 'fontSize': 16}
    ),

    dcc.Interval(
        id='interval-component-slow',
        interval=1*10000, # in milliseconds
        n_intervals=0
    )
    ], style={'padding': '20px'})

cred = credentials.Certificate('D:\VSCode\\2022-2023\Sesmeter-1\BDA_Data_Streaming_Real_Time\Tweet-Streaming\src\keys\\twitter-streaming-adminsdk.json')
# Initialize the app with a service account, granting admin privileges
firebase_admin.initialize_app(cred, {
    'databaseURL': "https://twitter-streaming-fc403-default-rtdb.firebaseio.com/"
})

# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph', 'children'),
              [Input('interval-component-slow', 'n_intervals')])
def update_graph_live(n):

  
    ref =db.reference('object')
    docs=ref.get()
    data=[]
    for key,val in docs.items():
        data.append(val)
        
    # df=pd.json_normalize(data)
    df=pd.DataFrame(data)
    # Convert UTC into PDT
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Clean and transform data to enable time series
    result = df.groupby([pd.Grouper(key='created_at', freq='10s'), 'polarity']).count().unstack(fill_value=0).stack().reset_index()
    result = result.rename(columns={"id": "Num of '{}' mentions".format(settings.TRACK_WORDS[0]), "created_at":"Time"})  
    time_series = result["Time"][result['polarity']==0].reset_index(drop=True)

    min10 = (datetime.datetime.now() - datetime.timedelta(hours=7 ,minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    min20 = (datetime.datetime.now() - datetime.timedelta(hours=7,minutes=20)).strftime("%Y-%m-%d %H:%M:%S")

    neu_num = result[result['Time']>min10]["Num of '{}' mentions".format(settings.TRACK_WORDS[0])][result['polarity']==0].sum()
    neg_num = result[result['Time']>min10]["Num of '{}' mentions".format(settings.TRACK_WORDS[0])][result['polarity']==-1].sum()
    pos_num = result[result['Time']>min10]["Num of '{}' mentions".format(settings.TRACK_WORDS[0])][result['polarity']==1].sum()


    count_now = df[df['created_at'] > min10]['id'].count()
    count_before = df[ (min20 < df['created_at']) & (df['created_at'] < min10)]['id'].count() 
    percent = (np.uint64(count_now-count_before)/np.uint64(count_before))*100

    ref_backUp =db.reference('Backup')
    back_up=ref_backUp.get()
    a={}
    for key,val in back_up.items() :
        a[key]=val
    

    back_up_dict =pd.DataFrame(a,index=[0])

    daily_tweets_num = int(back_up_dict['daily_tweets_num'].iloc[0] + result[-6:-3]["Num of '{}' mentions".format(settings.TRACK_WORDS[0])].sum())
    daily_impressions = int(back_up_dict['impressions'].iloc[0] + df[df['created_at'] > (datetime.datetime.now() - datetime.timedelta(hours=7,minutes=10))]['user_followers_count'].sum())
    
    PDT_now = datetime.datetime.now() - datetime.timedelta(minutes=3)
    if PDT_now.strftime("%H%M")=='0000':
        # cur.execute("UPDATE Back_Up SET daily_tweets_num = 0, impressions = 0;")
       ref_backUp.update({'daily_tweets_num': 0,'impressions':0})

    else:
        # cur.execute("UPDATE Back_Up SET daily_tweets_num = {}, impressions = {};".format(daily_tweets_num, daily_impressions))
        ref_backUp.update({'daily_tweets_num':daily_tweets_num,\
                            'impressions':daily_impressions})
    children = [
                html.Div([
                    html.Div([
                        dcc.Graph(
                            id='crossfilter-indicator-scatter',
                            figure={
                                'data': [
                                    go.Scatter(
                                        x=time_series,
                                        y=result["Num of '{}' mentions".format(settings.TRACK_WORDS[0])][result['polarity']==0].reset_index(drop=True),
                                        name="Neutrals",
                                        opacity=0.8,
                                        mode='lines',
                                        line=dict(width=0.5, color='rgb(131, 90, 241)'),
                                        stackgroup='one' 
                                    ),
                                    go.Scatter(
                                        x=time_series,
                                        y=result["Num of '{}' mentions".format(settings.TRACK_WORDS[0])][result['polarity']==-1].reset_index(drop=True).apply(lambda x: -x),
                                        name="Negatives",
                                        opacity=0.8,
                                        mode='lines',
                                        line=dict(width=0.5, color='rgb(255, 50, 50)'),
                                        stackgroup='two' 
                                    ),
                                    go.Scatter(
                                        x=time_series,
                                        y=result["Num of '{}' mentions".format(settings.TRACK_WORDS[0])][result['polarity']==1].reset_index(drop=True),
                                        name="Positives",
                                        opacity=0.8,
                                        mode='lines',
                                        line=dict(width=0.5, color='rgb(184, 247, 212)'),
                                        stackgroup='three' 
                                    )
                                ]
                            }
                        )
                    ], style={'width': '73%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
                    
                    html.Div([
                        dcc.Graph(
                            id='pie-chart',
                            figure={
                                'data': [
                                    go.Pie(
                                        labels=['Positives', 'Negatives', 'Neutrals'], 
                                        values=[pos_num, neg_num, neu_num],
                                        name="View Metrics",
                                        marker_colors=['rgba(184, 247, 212, 0.6)','rgba(255, 50, 50, 0.6)','rgba(131, 90, 241, 0.6)'],
                                        textinfo='value',
                                        hole=.65)
                                ],
                                'layout':{
                                    'showlegend':False,
                                    'title':'Tweets In Last 3 Mins',
                                    'annotations':[
                                        dict(
                                            text='{0:.1f}'.format((pos_num+neg_num+neu_num)),
                                            font=dict(
                                                size=40
                                            ),
                                            showarrow=False
                                        )
                                    ]
                                }

                            }
                        )
                    ], style={'width': '27%', 'display': 'inline-block'})
                ]),
                
                html.Div(
                    className='row',
                    children=[
                        html.Div(
                            children=[
                                html.P('Tweets/10 Mins Changed By',
                                    style={
                                        'fontSize': 25
                                    }
                                ),
                                html.P('{0:.2f}%'.format(percent) if percent <= 0 else '+{0:.2f}%'.format(percent),
                                    style={
                                        'fontSize': 40
                                    }
                                )
                            ], 
                            style={
                                'width': '60%',
                                'display': 'block'
                            }

                        ),
                        html.Div(
                            children=[
                                html.P('Potential Impressions Today',
                                    style={
                                        'fontSize': 25
                                    }
                                ),
                                html.P('{0:.1f}K'.format(daily_impressions/10) \
                                        if daily_impressions < 100 else \
                                            ('{0:.1f}M'.format(daily_impressions/100) if daily_impressions < 100 \
                                            else '{0:.1f}B'.format(daily_impressions/100)),
                                    style={
                                        'fontSize': 40
                                    }
                                )
                            ], 
                            style={
                                'width': '30%',
                                'display': 'inline-block'
                            }
                        ),
                        html.Div(
                            children=[
                                html.P('Tweets Posted Today',
                                    style={
                                        'fontSize': 25
                                    }
                                ),
                                html.P('{0:.1f}'.format(daily_tweets_num),
                                    style={
                                        'fontSize': 40
                                    }
                                )
                            ], 
                            style={
                                'width': '20%', 
                                'display': 'inline-block'
                            }
                        ),

                        html.Div(
                            children=[
                                html.P("Currently tracking \"Facebook\" brand (NASDAQ: FB) on Twitter in Pacific Daylight Time (PDT).",
                                    style={
                                        'fontSize': 25
                                    }
                                ),
                            ], 
                            style={
                                'width': '40%', 
                                'display': 'inline-block',
                                'align' : 'center'
                            }
                        ),

                    ],
                    style={'marginLeft': 70}
                )
            ]
    return children

@app.callback(Output('live-update-graph-bottom', 'children'),
              [Input('interval-component-slow', 'n_intervals')])
def update_graph_bottom_live(n):

   
    ref =db.reference('object')
    docs=ref.get()
    data=[]
    for key,val in docs.items():
        data.append(val)
        
    df=pd.json_normalize(data)

    # Convert UTC into PDT
    df['created_at'] = pd.to_datetime(df['created_at'])


    # Clean and transform data to enable word frequency
    content = ' '.join(df["text"])
    content = re.sub(r"http\S+", "", content)
    content = content.replace('RT ', ' ').replace('&amp;', 'and')
    content = re.sub('[^A-Za-z0-9]+', ' ', content)
    content = content.lower()

    # Filter constants for states in US
    STATES = ['Alabama', 'AL', 'Alaska', 'AK', 'American Samoa', 'AS', 'Arizona', 'AZ', 'Arkansas', 'AR', 'California', 'CA', 'Colorado', 'CO', 'Connecticut', 'CT', 'Delaware', 'DE', 'District of Columbia', 'DC', 'Federated States of Micronesia', 'FM', 'Florida', 'FL', 'Georgia', 'GA', 'Guam', 'GU', 'Hawaii', 'HI', 'Idaho', 'ID', 'Illinois', 'IL', 'Indiana', 'IN', 'Iowa', 'IA', 'Kansas', 'KS', 'Kentucky', 'KY', 'Louisiana', 'LA', 'Maine', 'ME', 'Marshall Islands', 'MH', 'Maryland', 'MD', 'Massachusetts', 'MA', 'Michigan', 'MI', 'Minnesota', 'MN', 'Mississippi', 'MS', 'Missouri', 'MO', 'Montana', 'MT', 'Nebraska', 'NE', 'Nevada', 'NV', 'New Hampshire', 'NH', 'New Jersey', 'NJ', 'New Mexico', 'NM', 'New York', 'NY', 'North Carolina', 'NC', 'North Dakota', 'ND', 'Northern Mariana Islands', 'MP', 'Ohio', 'OH', 'Oklahoma', 'OK', 'Oregon', 'OR', 'Palau', 'PW', 'Pennsylvania', 'PA', 'Puerto Rico', 'PR', 'Rhode Island', 'RI', 'South Carolina', 'SC', 'South Dakota', 'SD', 'Tennessee', 'TN', 'Texas', 'TX', 'Utah', 'UT', 'Vermont', 'VT', 'Virgin Islands', 'VI', 'Virginia', 'VA', 'Washington', 'WA', 'West Virginia', 'WV', 'Wisconsin', 'WI', 'Wyoming', 'WY']
    STATE_DICT = dict(itertools.zip_longest(*[iter(STATES)] * 2, fillvalue=""))
    INV_STATE_DICT = dict((v,k) for k,v in STATE_DICT.items())

    # Clean and transform data to enable geo-distribution
    is_in_US=[]
    geo = df[['user_location']]
    df = df.fillna(" ")
    for x in df['user_location']:
        check = False
        for s in STATES:
            if s in x:
                is_in_US.append(STATE_DICT[s] if s in STATE_DICT else s)
                check = True
                break
        if not check:
            is_in_US.append(None)

    geo_dist = pd.DataFrame(is_in_US, columns=['State']).dropna().reset_index()
    geo_dist = geo_dist.groupby('State').count().rename(columns={"index": "Number"}) \
        .sort_values(by=['Number'], ascending=False).reset_index()
    geo_dist["Log Num"] = geo_dist["Number"].apply(lambda x: math.log(x, 2))


    geo_dist['Full State Name'] = geo_dist['State'].apply(lambda x: INV_STATE_DICT[x])
    geo_dist['text'] = geo_dist['Full State Name'] + '<br>' + 'Num: ' + geo_dist['Number'].astype(str)


    tokenized_word = word_tokenize(content)
    stop_words=set(stopwords.words("english"))
    filtered_sent=[]
    for w in tokenized_word:
        if (w not in stop_words) and (len(w) >= 3):
            filtered_sent.append(w)
    fdist = FreqDist(filtered_sent)
    fd = pd.DataFrame(fdist.most_common(16), columns = ["Word","Frequency"]).drop([0]).reindex()
    fd['Polarity'] = fd['Word'].apply(lambda x: TextBlob(x).sentiment.polarity)
    fd['Marker_Color'] = fd['Polarity'].apply(lambda x: 'rgba(255, 50, 50, 0.6)' if x < -0.1 else \
        ('rgba(184, 247, 212, 0.6)' if x > 0.1 else 'rgba(131, 90, 241, 0.6)'))
    fd['Line_Color'] = fd['Polarity'].apply(lambda x: 'rgba(255, 50, 50, 1)' if x < -0.1 else \
        ('rgba(184, 247, 212, 1)' if x > 0.1 else 'rgba(131, 90, 241, 1)'))



    # Create the graph 
    children = [
                html.Div([
                    dcc.Graph(
                        id='x-time-series',
                        figure = {
                            'data':[
                                go.Bar(                          
                                    x=fd["Frequency"].loc[::-1],
                                    y=fd["Word"].loc[::-1], 
                                    name="Neutrals", 
                                    orientation='h',
                                    marker_color=fd['Marker_Color'].loc[::-1].to_list(),
                                    marker=dict(
                                        line=dict(
                                            color=fd['Line_Color'].loc[::-1].to_list(),
                                            width=1),
                                        ),
                                )
                            ],
                            'layout':{
                                'hovermode':"closest"
                            }
                        }        
                    )
                ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
                html.Div([
                    dcc.Graph(
                        id='y-time-series',
                        figure = {
                            'data':[
                                go.Choropleth(
                                    locations=geo_dist['State'], # Spatial coordinates
                                    z = geo_dist['Log Num'].astype(float), # Data to be color-coded
                                    locationmode = 'USA-states', # set of locations match entries in `locations`
                                    #colorscale = "Blues",
                                    text=geo_dist['text'], # hover text
                                    geo = 'geo',
                                    colorbar_title = "Num in Log2",
                                    marker_line_color='white',
                                    colorscale = ["#fdf7ff", "#835af1"],
                                    #autocolorscale=False,
                                    #reversescale=True,
                                ) 
                            ],
                            'layout': {
                                'title': "Geographic Segmentation for US",
                                'geo':{'scope':'usa'}
                            }
                        }
                    )
                ], style={'display': 'inline-block', 'width': '49%'})
            ]
        
    return children
    

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8050)
    app.run_server(debug=True)