from numpy.core.arrayprint import DatetimeFormat
import pandas as pd
import numpy as np

from pathlib import Path

def reindex_dates_to_hour():
    file_url = "gridwatch 2017-2018 5mins.csv"

    df = pd.read_csv(file_url, engine='python', parse_dates=True, index_col=0)

    df.index = pd.to_datetime(df.index)

    df = df[~df.index.duplicated(keep='first')]

    df = df.resample('H').sum()
  
    df.to_csv("output.csv",header=["demand"])

# reindex_dates_to_hour()


''' county namesfdf'''
# town_names = ["Wiltshire-Lyneham", "Gloucestershire-Little Rissington", "Cornwall-Camborne", "Devon-Exeter", "Cornwall-Cardinham Bodmin", "Somerset-Liscombe", "Somerset-Yeovilton",
#               "Dorset-Hurn", "Hampshire-Odiham", "Surrey-Wisley", "Kent-East Malling", "Kent-Manston", "Greater London-Heathrow", "Greater London-Kew Gardens", "Greater London-Kenley"]

# town_names = ["Wiltshire-Lyneham", "Gloucestershire-Little Rissington", "Cornwall-Camborne", "Devon-Exeter", "Cornwall-Cardinham Bodmin", "Somerset-Liscombe", "Somerset-Yeovilton",
#               "Dorset-Hurn", "Hampshire-Odiham", "Surrey-Wisley", "Kent-East Malling", "Kent-London", "Greater London-Heathrow", "Greater London-Kew Gardens", "Greater London-Kenley"]


town_names = ["South East", "South West", "Outer London" ]
def get_counties(town_names,df):

    local_auth_list = df['Local Authority'].tolist()

    combined = '\t'.join(local_auth_list)
    indexes = []

    for i in range(len(town_names)):
        town = town_names[i]
        for j in range(len(local_auth_list)):
            county = local_auth_list[j]
            town = town.replace('-', ' ')
            town_tokens = town.split(' ')
            for t in town_tokens:
                if (t in county):
                    # print(town, " fdsf ", county, i, j)
                    indexes.append(j)
                    break
            
            else:

                continue  # only executed if the inner loop did NOT break

            break  #
    return indexes


def get_counties_match_exact(town_names,df):
    indexes=[]

    for town in town_names:

        indexes.append(df[df['Local Authority'] == town].index[0])
    return indexes
    
def create_local_demand_timeseries(time_series_df,local_auth_df,place_name):
    #assert df.loc[df['Local Authority'] == "Wiltshire"].shape, (1,1)
    #assert time_series_df["demand"].sum() !=0 , True

    factor= (local_auth_df.loc[local_auth_df['Local Authority'] == place_name]["Total_GWh"])/(time_series_df["demand"].sum())

    #assert factor.shape, (12,2)

    time_series_df["demand"] = factor.values[0]* time_series_df["demand"]
    time_series_df.to_csv("local_demand_timeseries/"+place_name+".csv",index=False)


local_auth_df = pd.read_csv("Subnational_electricity_consumption_statistics_2017.csv",)

#indexes=get_counties(town_names,local_auth_df)
indexes=get_counties_match_exact(town_names,local_auth_df)



local_auth_df=local_auth_df.loc[indexes]
local_auth_df = local_auth_df[~local_auth_df.index.duplicated(keep='first')]
local_auth_df = local_auth_df[['Local Authority', 'Total_GWh']]
# TODO use domestic, non-domestic, total seperately



time_series_df=pd.read_csv("output.csv")

Path("local_demand_timeseries1").mkdir(parents=True, exist_ok=True)

for place in local_auth_df['Local Authority'].tolist():
    create_local_demand_timeseries(time_series_df,local_auth_df,place)



