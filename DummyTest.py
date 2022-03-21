import json
import pkgutil
from datetime import datetime
from itertools import repeat
from multiprocessing import Pool

import pandas as pd
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import bulk

from Investar.config.configurations import *
from Investar.logger import *




if __name__ == '__main__':

    url = f"https://fchart.stock.naver.com/sise.nhn?symbol=010960&timeframe=day&count=5&requestType=0"

    get_result = requests.get(url)
    bs_obj = BeautifulSoup(get_result.content, "html.parser")

    print(bs_obj)


    #url_market = 'kosdaqMkt'
    url_market = 'stockMkt'

    # https://lifeonroom.com/study-lab/get-stock-code-price/ 참고 URL
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=%s' % url_market
    krx_kospi = pd.read_html(url, header=0)[0]

    krx_kospi = krx_kospi[['종목코드', '회사명']]
    krx_kospi = krx_kospi.rename(columns={'종목코드': 'code', '회사명': 'company'})
    krx_kospi.code = krx_kospi.code.map('{:06d}'.format)
    krx_kospi['market_code'] = 'KOSPI'

    # for idx in range(len(krx_kospi)):
    #     print(f'code : {krx_kospi.code.values[idx]}, name : {krx_kospi.company.values[idx]}, market_cod : {krx_kospi.market_code.values[idx]}')

    print(len(krx_kospi))

    url_market = 'kosdaqMkt'
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=%s' % url_market
    krx_kosdaq = pd.read_html(url, header=0)[0]

    krx_kosdaq = krx_kosdaq[['종목코드', '회사명']]
    krx_kosdaq = krx_kosdaq.rename(columns={'종목코드': 'code', '회사명': 'company'})
    krx_kosdaq.code = krx_kosdaq.code.map('{:06d}'.format)
    krx_kosdaq['market_code'] = 'KOSDAQ'

    # for idx in range(len(krx_kospi)):
    #     print(
    #         f'code : {krx_kosdaq.code.values[idx]}, name : {krx_kosdaq.company.values[idx]}, market_cod : {krx_kosdaq.market_code.values[idx]}')

    print(len(krx_kosdaq))

    krx = pd.concat([krx_kospi, krx_kosdaq])

    print(len(krx))

    for index, row in krx.iterrows():
        if row['company'] in ['SV인베스트먼트', 'TPC', 'TS인베스트먼트','YBM넷','iMBC', '넥스트사이언스', '노루홀딩스','대상홀딩스']:
            print(row['code'], row['company'], row['market_code'])

    # code: 289080, name: SV인베스트먼트, market_cod: KOSDAQ
    # code: 04
    # 8770, name: TPC, market_cod: KOSDAQ
    # code: 246690, name: TS인베스트먼트, market_cod: KOSDAQ
    # code: 057030, name: YBM넷, market_cod: KOSDAQ
    # code: 052220, name: iMBC, market_cod: KOSDAQ
    # code: 07
    # 8890, name: 가온미디어, market_cod: KOSDAQ
    # code: 0630
    # 80, name: 게임빌, market_cod: KOSDAQ

    # code: 0035
    # 80, name: 넥스트사이언스, market_cod: KOSPI
    # code: 000320, name: 노루홀딩스, market_cod: KOSPI
    # code: 0004
    # 90, name: 대동, market_cod: KOSPI
    # code: 0
    # 84690, name: 대상홀딩스, market_cod: KOSPI
