import pandas as pd
from bs4 import BeautifulSoup
import requests, calendar, time, json
from datetime import datetime
from threading import Timer
from elasticsearch import Elasticsearch
from multiprocessing import Process, Queue
import Investar.DataManager as dm


result = Queue()
checkSet = set()
procs = []

def worker(sub_codes_list, pages_to_fetch, idx, result):
    print(f'Child[{idx}], sub_codes_list[{len(sub_codes_list)}], pages_to_fetch[{pages_to_fetch}]')
    child = dm.ESManager()
    child.update_daily_price(sub_codes_list,pages_to_fetch)
    result.put(idx)


if __name__ == '__main__':
    codes = dm.update_comp_info()
    print(len(codes))
    pages_to_fetch = dm.getPageToFetch()
    print(pages_to_fetch)
    sub_codes_list = dm.split_dict_equally(codes, 10)
    
    for idx in range(len(sub_codes_list)):
        proc = Process(target=worker, args=(sub_codes_list[idx], pages_to_fetch, idx, result))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()

    result.put('STOP')
    total = 0
    while True:
        tmp = result.get()
        if tmp == 'STOP':
            break
        else:
            total += tmp
            checkSet.add(tmp)

    print(f'Result: {total}')
    print(f'Set: {checkSet}')
    print(f'Set Length : {len(checkSet)}')


#http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
#update 참고 :https://nashorn.tistory.com/entry/Python%EC%9C%BC%EB%A1%9C-Elasticsearch-update-%EC%8B%A4%ED%96%89
# https://stackoverflow.com/questions/54589940/detecting-termination-of-multiple-child-processes-with-multiprocessing
# https://dailyheumsi.tistory.com/105
