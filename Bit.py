#!/usr/bin/env python
# coding: utf-8



from multiprocessing.pool import ThreadPool
import requests
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import urllib.request
import time
import gc
import csv


def call_1(start,end,path):

    start = start
    end = end
    path = path
    row = {}
    urls = []
    no_height = []
    sum_1=0
    df = pd.read_csv(path)
    len_df = len(df)
      
    df.drop_duplicates('Height',inplace=True)
    print('no_drop :',len_df,'drop :',len(df),'diff :',len_df-len(df))
    
    lst = [i for i in df.Height]
    for i in range(start,end):
        if i not in lst:
            no_height.append(i)
    
    
    for i in no_height :
        urls.append("https://www.blockchain.com/btc/block/{}".format(i)) 

    for url in urls:
        sum_1 = sum_1+1
        if sum_1 == 5000:
            break;
        try:
            request_1 = requests.get(url)
            soup = BeautifulSoup(request_1.content,'lxml')
            elements = soup.find_all(attrs={'class':"sc-1enh6xt-0 kiseLw"})
        except:
            df.to_csv(path,index=False, encoding='utf-8')
            print(url)
            return()
            
        for i in range(len(elements)):
            name = elements[i].find('span',attrs={'class':'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP sc-1n72lkw-0 ebXUGH'}).contents[0]
            try:
                value = elements[i].find('span',attrs={'class':'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'}).contents[0]
            except AttributeError:
                value = 'None'
            #print(name, ':', value)
            row[name] = value
            
        update = pd.Series(row)
        df = df.append(update,ignore_index=True)
        #soup.close()
    
    df.to_csv(path,index=False, encoding='utf-8')
    gc.collect()

