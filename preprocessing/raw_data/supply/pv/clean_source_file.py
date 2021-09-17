# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

import csv
import pandas as pd
import numpy as np
import glob


import download_source_file


def get_source_file(html_links,date_time):

    for i in range(len(html_links)):

        download_source_file.downloader(html_links[i])

        file_url = html_links[i].rsplit('/', 1)[-1]

        df = pd.read_csv("raw/"+file_url, skiprows=75,
                         skipfooter=1, engine='python',parse_dates=True,index_col=0)
        #TODO 75 is dodgy

        df1 = df[['glbl_irad_amt']]
        df1 = df1[~df1.index.duplicated(keep='first')]

        # df1.set_index('ob_end_time', inplace=True)

        # df1.index = pd.DatetimeIndex(df1.index)
        df1.index = pd.to_datetime(df1.index)
        # df2 = df1.resample('H').interpolate()

        # create a min max to make the df the uniform 

        idx = pd.date_range(date_time, periods=365*24, freq="H")
        # print(df1.info())
        # print(df1[df1.index.duplicated()])
        # df1.drop_duplicates()
        # print(df1[df1.index.duplicated()])

        df1.index = pd.DatetimeIndex(df1.index)

        df1 = df1.reindex(idx, fill_value=0)

        df1.to_csv("cleaned/" + file_url,index_label="ob_end_time")


def compile_all_csv_files():

    path = r'C:\Sohum\Home\CodeHome\Python\optimization_course\optimization\dataFiles\supply\pv\cleaned'  # use your path
    all_files = glob.glob(path + "/*.csv")

    df = pd.read_csv(all_files[0], index_col=None, header=0)

    time_col = df[['ob_end_time']]
    li = [time_col]

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        df= df[['glbl_irad_amt']]
        li.append(df)

    frame = pd.concat(li, axis=1, ignore_index=True)
    return frame

def get_html_links():

    lines = []
    with open('urls.txt') as f:
        lines = f.readlines()

    html_links1=[]
    county_names1=[]
    for i in range(0,len(lines),3):
        county_names1.append(lines[i].strip())
        html_links1.append(lines[i+1].strip())

    return html_links1,county_names1

       


def main(html_links, county_names,date_time):

    get_source_file(html_links,date_time)
    print("should be good to go hold on")

    df = compile_all_csv_files()
    columns = ['time' ]

    columns=columns+county_names
    
    df.columns = columns
    df.to_csv("outputs/1.csv",index=False)



# html_links1 = [
#     "https://dap.ceda.ac.uk/badc/ukmo-midas-open/data/uk-radiation-obs/dataset-version-201908/greater-london/00708_heathrow/qc-version-1/midas-open_uk-radiation-obs_dv-201908_greater-london_00708_heathrow_qcv-1_2006.csv", "https://dap.ceda.ac.uk/badc/ukmo-midas-open/data/uk-radiation-obs/dataset-version-201908/greater-london/00708_heathrow/qc-version-1/midas-open_uk-radiation-obs_dv-201908_greater-london_00708_heathrow_qcv-1_2007.csv"

# ]
# county_names1 = ["Wiltshire-Lyneham", "Goucestershire-Little Rissington"]



html_links1,county_names1=get_html_links()
main(html_links1, county_names1,"2017-01-01")
