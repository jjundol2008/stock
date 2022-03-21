
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

es = Elasticsearch()

class DataLinKer():
    def __init__(self, hostGubun):
        self.logging = Logging()
        self.esurls = getESHost(hostGubun)
        self.port = getESPort(hostGubun)

        global es
        #print(self.esurls)
        #print(self.port)
        es = Elasticsearch(self.esurls, port=self.port, timeout=30, max_retries=10, retry_on_timeout=True)
        self.company_index_name = getIndexName('company_index_name')  # 회사정보를 저장할 index의 이름을 가져온다.
        self.price_index_name = getIndexName('price_index_name') # daily price를 저장한 index의 이름을 가져온다.

        self.logging.logger.debug(f'### es urls : {self.esurls}')
        self.logging.logger.debug(f'### company index name : {self.company_index_name}')
        self.logging.logger.debug(f'### price index name : {self.price_index_name}' )
        self.processNum = 1

        # company_info index가 없을 경우 생성한다.
        if es.indices.exists(index=self.company_index_name) == False:
            self.logging.logger.debug('create company_info index')

            data = pkgutil.get_data(__name__, 'templates/comp_mapping.json')
            mapping = json.loads(data.decode())
            es.indices.create(index=self.company_index_name, body=mapping)
        else:
            self.logging.logger.debug('index for company_info exists ')

        # daily_price index가 없을 경우 생성한다.
        if es.indices.exists(index=self.price_index_name) == False:
            self.logging.logger.debug('create daily_price index')

            data = pkgutil.get_data(__name__, 'templates/price_mapping.json')
            mapping = json.loads(data.decode())
            es.indices.create(index=self.price_index_name, body=mapping)
        else:
            self.logging.logger.debug('index for daily_price exists ')

        self.codes = dict()

    def __del__(self):
        pass
        # 소멸자: ES 연결 해제.
        #global es.close()

    def update_comp_info(self):
        """종목코드를 company_info index에 업데이트한 후 딕셔너리에 저장"""
        #index = 'company_info'
        body = {
            "query": {
                "match_all": {}
            },
            "size": 10000
        }

        results = es.search(index=self.company_index_name, body=body)

        for result in results['hits']['hits']:
            self.codes[result['_source']['code']] = result['_source']['company']

        #self.logging.logger.debug((f'codes\'s size is [{len(self.codes)}]')

        body = {
            "aggs": {
                "max_date": {
                    "max": {
                        "field": "last_update",
                        "format": "yyyy-MM-dd"
                    }
                }
            },
            "size": 0
        }

        today = datetime.datetime.today().strftime('%Y-%m-%d')
        #today  = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
        results = es.search(index=self.company_index_name, body=body)

        #print(body)
        if results['aggregations']['max_date']['value'] == None or results['aggregations']['max_date'][
            'value_as_string'] < today:
            #krx = self.read_krx_code()
            krx = self.read_krx_code_api()

            # print(krx)
            company_codes = []
            company_names = []
            market_codes = []


            for idx in range(len(krx)):
                company_codes.append(krx.code.values[idx])
                company_names.append(krx.company.values[idx])
                market_codes.append(krx.market_code.values[idx])
                self.codes[krx.code.values[idx]] = krx.company.values[idx]


            df = pd.DataFrame([ x for x in zip(company_codes, company_names, market_codes)], columns=['code','company','market_code'])
            doc = [
                {
                    "_index": self.company_index_name,
                    "_id": x[0],
                    "_source": {
                        "code":x[0],
                        "company":x[1],
                        "market_code":x[2],
                        "last_update": today
                    }
                }
                for x in zip(df['code'], df['company'], df['market_code'])
            ]

            helpers.bulk(es, doc)

            self.logging.logger.debug('inserting codes finished!!!')

        else:
            if results['aggregations']['max_date']['value'] != None:
                self.logging.logger.debug('max_date is not None')

            if results['aggregations']['max_date']['value_as_string'] >= today:
                self.logging.logger.debug('max_date is not less than today')


    def read_krx_code_api(self):
        """ KRX로 부터 상장 법인 목록 API를 읽어와서 데이터 프레임으로 변환 , KOSPI, KOSDAQ 구분까지 입력"""
        url_market = 'stockMkt'
        # https://lifeonroom.com/study-lab/get-stock-code-price/ 참고 URL
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=%s' % url_market
        krx_kospi = pd.read_html(url, header=0)[0]
        krx_kospi = krx_kospi[['종목코드', '회사명']]
        krx_kospi = krx_kospi.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx_kospi.code = krx_kospi.code.map('{:06d}'.format)
        krx_kospi['market_code'] = 'KOSPI'

        url_market = 'kosdaqMkt'
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=%s' % url_market
        krx_kosdaq = pd.read_html(url, header=0)[0]
        krx_kosdaq = krx_kosdaq[['종목코드', '회사명']]
        krx_kosdaq = krx_kosdaq.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx_kosdaq.code = krx_kosdaq.code.map('{:06d}'.format)
        krx_kosdaq['market_code'] = 'KOSDAQ'

        krx = pd.concat([krx_kospi, krx_kosdaq])

        return krx

    #### 구메소드
    # def read_krx_code(self):
    #     """KRX로 부터 상장법인 목록 파일을 읽어와서 데이터프레임으로 변환"""
    #     url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
    #           'download&searchType=13'
    #     krx = pd.read_html(url, header=0)[0]
    #     krx = krx[['종목코드', '회사명']]
    #     krx = krx.rename(columns={'종목코드':'code','회사명':'company'})
    #     krx.code = krx.code.map('{:06d}'.format)
    #     #print(krx)
    #     return krx

    #def update_daily_price(self, codes):
    def update_daily_price(self, codes, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""

        for idx, code in enumerate(codes):
            df = self.read_naver(code, codes[code], pages_to_fetch)
            if df is None:
                continue
            ies = Elasticsearch(self.esurls, port=self.port,  timeout=30, max_retries=10, retry_on_timeout=True)

            self.replace_into_index(df, idx, code, codes[code], ies)


    def read_naver(self, code, company, pages_to_fetch=1):
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환 """
        # count=3000에서 3000은 과거 3,000 영업일간의 데이터를 의미. 사용자가 조절 가능

        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count={pages_to_fetch+1}&requestType=0"

        get_result = requests.get(url)
        bs_obj = BeautifulSoup(get_result.content, "html.parser")

        # information
        inf = bs_obj.select('item')
        columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df_inf = pd.DataFrame([], columns=columns, index=range(len(inf)))

        for i in range(len(inf)):
            df_inf.iloc[i] = str(inf[i]['data']).split('|')

        df_inf['date'] = pd.to_datetime(df_inf['date']).dt.strftime('%Y-%m-%d')
        #df_inf['date'] = df_inf['date'].dt.strftime('%Y-%m-%d')
        #print(df_inf)

        df_inf[['close', 'open', 'high', 'low', 'volume']] = df_inf[['close', 'open', 'high', 'low', 'volume']].astype(int)
        dff_inf = df_inf['close'].diff().abs()
        df_inf['diff'] = dff_inf
        df_inf = df_inf.dropna()
        df_inf = df_inf[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]



        return df_inf

    def replace_into_index(self, df, num, code, company, ies):
        """네이버에서 읽어온 주식 시세를 DB에 REPLACE """

        df = pd.DataFrame(data={'code': code,
                                'date': df['date'],
                                'close': df['close'],
                                'diff': df['diff'],
                                'high': df['high'],
                                'low': df['low'],
                                'open': df['open'],
                                'volume': df['volume'],
                                '_id': code + '-' + df['date']})
        documents = df.to_dict(orient='records')

        # print(documents)
        # bulk(ies, documents, index = index, doc_type='_doc', raise_on_error=True)
        bulk(ies, documents, index=self.price_index_name, raise_on_error=True)

    #def run(self, sub_codes_listprocessNum=1):
    def run(self, processNum=1, pages_to_fetch=1):

        sub_codes_list = self.split_codes_equally(processNum)
        pool = Pool(processes=processNum)
        #pool.map(self.update_daily_price, sub_codes_list)
        #pool.map(self.update_daily_price, zip(sub_codes_list, repeat(pages_to_fetch)))
        pool.starmap(self.update_daily_price, zip(sub_codes_list, repeat(pages_to_fetch)))
        pool.close()
        pool.join


    def split_codes_equally(self, chunks=1):
        "Splits dict by kes. Returna a list of dictionaries"
        # prep with empty dicts
        return_list = [dict() for idx in range(chunks)]
        idx = 0
        for k,v in self.codes.items():
            return_list[idx][k] = v
            if idx < chunks-1: # indexes start at 0
                idx += 1
            else:
                idx = 0
        return return_list

    def getCodes(self):
        return self.codes


    def delete_index(index):
        es.indices.delete(index=index, ignore=[400, 404])

    def setPorcessNum(self, num=1):
        self.processNum = num