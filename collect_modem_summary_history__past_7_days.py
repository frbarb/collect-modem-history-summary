#!/usr/bin/python

import sys
import csv
import re
import os
import glob
import time
from urllib.request import HTTPBasicAuthHandler
import pandas as pd
import zipfile
import datetime
import shutil
import requests
import json
import math


# green flag
genstart = time.time()

# prepare environment
def env_setup():
    envstart = time.time()
    today = datetime.datetime.now()
    date_time = today.strftime('%Y-%m-%d_%H%M')
    if not os.path.exists(date_time):
        os.mkdir(date_time)

    print("\nStartTime")
    print(today)

    global cwd
    cwd = os.getcwd()
    print("\nCurrent working directory is\n" + cwd)
    
    global log
    log = (cwd + "\\" + date_time + "\\modem_summ_hist\\log\\")
    if not os.path.exists(log):
        os.makedirs(log)
    
    global logfile
    logfile = (log + "logfile.txt")
    #sys.stdout = open(logfile, 'w')
    
    global collected_files
    collected_files = (cwd + "\\" + date_time + "\\modem_summ_hist\\collected_files\\")
    if not os.path.exists(collected_files):
        os.makedirs(collected_files)    
    
    global modem_summ_hist
    modem_summ_hist = (cwd + "\\" + date_time + "\\modem_summ_hist\\")
           
    print("\nTempo total preparando env:")
    print("%s seconds" % (time.time() - envstart))

# collect all blocks from modem summary history api from every server
def collect():
   
    server_ip_dict = {'CITY': 'xxx.xxx.xxx.xxx'}

    # define loop count according to totRecord and block length    
    collection_start = time.time()

    for key, value in server_ip_dict.items():        
        print(key)
        block_length = 50000
        try:
            url = 'http://' + value + '/api/modemSummary/kpi/summary/history?BlockLength=' + str(block_length) + '&BlockOffset=0&sampleResponse=false'
            r_API = requests.get(url, auth=('admin', 'admin@xptbrasil'))
            r_json = r_API.json()
            tot_records = r_json['totRecord']
            loop_count = (int(math.ceil(tot_records / block_length)))
            df = pd.json_normalize(r_json, record_path =['modems'])
            output_file = collected_files + key + '_output_modem_history.csv'

            # build a 7 past dates list                        
            past_days_list = []
            for i in range(8):
                past_day = (datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                past_days_list.append(past_day)
            print(past_days_list)
            
            # build block offset list as per totRecord found
            block_offset_list = []            
            for i in range(loop_count):          
                block_offset = block_length * i
                block_offset_list.append(block_offset)                

            print('......................................................')
            print('the current offset list is............................ ')
            print(block_offset_list)
            
            # loop through past 7 days data, to append into dataframe
            for past_day in past_days_list[1:]:
                print('......................................................')
                print('current past date is ................ ' + str(past_day))
                print(past_day)
                # loop through offsets to append to current dataframe                
                if not block_offset_list:
                    _offset = 0
                    url_past_days = 'http://' + value + '/api/modemSummary/kpi/summary/history?BlockLength=' + str(block_length) + '&BlockOffset=' + str(_offset) + '&TotalRecords=' + str(tot_records) + '&date=' + past_day + '&sampleResponse=false'
                    r_API_past_days = requests.get(url_past_days, auth=('admin', 'admin@xptbrasil'))    
                    r_json_past_days = r_API_past_days.json()
                    df1 = pd.json_normalize(r_json_past_days, record_path =['modems'])
                    df = df.append(df1)
                    print('partial df is below this line ................ the current offset is ' + str(_offset))
                    print(df.head)
                else:
                    for _offset in block_offset_list[1:]:
                        print('the current offset is............................ ' + str(_offset))
                        url_past_days = 'http://' + value + '/api/modemSummary/kpi/summary/history?BlockLength=' + str(block_length) + '&BlockOffset=' + str(_offset) + '&TotalRecords=' + str(tot_records) + '&date=' + past_day + '&sampleResponse=false'
                        r_API_past_days = requests.get(url_past_days, auth=('admin', 'admin@xptbrasil'))    
                        r_json_past_days = r_API_past_days.json()
                        df1 = pd.json_normalize(r_json_past_days, record_path =['modems'])
                        df = df.append(df1)
                        print('partial df is below this line ................ the current offset is ' + str(_offset))
                        print(df.head)

            # print results to check while in execution
            print('final df is below this line ................')
            print(df.head)
            df.to_csv(output_file, sep=',', encoding='utf-8', index=False)
            print("\nTempo coletando......: " + key)
            print("%s seconds" % (time.time() - collection_start))

        except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.Timeout, requests.exceptions.RequestException, requests.exceptions.RetryError) as e:
            print(e)
            continue

# total time taken
def total_time():
    print("\nTempo total gasto:")
    print("%s seconds" % (time.time() - genstart))
    print("Finished!\n")

# main func
def main():
    env_setup()
    collect()
    total_time()

# call 2x safe lock
if __name__ == '__main__':
    main()