import pandas as pd
from bs4 import BeautifulSoup
import requests, calendar, time, json
from datetime import datetime
from threading import Timer
from elasticsearch import Elasticsearch
from multiprocessing import Process, Pool
import pkgutil
from elasticsearch.helpers import bulk
import os
import time


es = Elasticsearch(['192.168.0.13','192.168.0.14','192.168.0.15'], port=9200, timeout=30, max_retries=10, retry_on_timeout=True)

class A():
    def __init__(self, vl):
        """생성자: ES 연결 및 종목코드 딕셔너리 생성 """
        self.vl = vl
        print('init start')
             
        """company_info index가 없을 경우 생성한다. """
        if es.indices.exists(index='company_info') == False:
            print('create company_info index')
            index='company_info'

            #data = pkgutil.get_data(__name__, 'templates/comp_mapping.json')
            #mapping = json.loads(data.decode())
            with open('comp_mapping.json', 'r') as f:
                mapping = json.load(f)
            
            es.indices.create(index=index, body=mapping)

        else:
            print('company_info exists ')

        """ daily_price index가 없을 경우 생성한다. """
        if es.indices.exists(index='daily_price') == False:
            print('create daily_price index')
            index='daily_price'

            #data = pkgutil.get_data(__name__, 'templates/price_mapping.json')
            #mapping = json.loads(data.decode())

            with open('price_mapping.json', 'r') as f:
                mapping = json.load(f)

            es.indices.create(index=index, body=mapping)

        else:
            print('daily_price exists ')

        self.codes = dict()

    def __del__(self):
        """소멸자: ES 연결 해제"""
        print('end')

    def update_comp_info(self):
        """종목코드를 company_info index에 업데이트한 후 딕셔너리에 저장"""
        index = 'company_info'
        body = {
          "query": {
            "match_all": {}
          },
          "size": 10000
        }

        results = es.search(index=index, body=body)

        for result in results['hits']['hits']:
            self.codes[result['_source']['code']] = result['_source']['company']

        print(f'codes\'s size is [{len(self.codes)}]')

        body = {
            "aggs": {
                "max_date":{
                    "max":{
                        "field": "last_update",
                        "format": "yyyy-MM-dd"
                    }
                }
            },
            "size": 0
        }

        today = datetime.today().strftime('%Y-%m-%d')
        results = es.search(index=index, body=body)
        
        if results['aggregations']['max_date']['value'] == None or results['aggregations']['max_date']['value_as_string'] < today:
            krx = self.read_krx_code()
            for idx in range(len(krx)):
                code = krx.code.values[idx]
                company = krx.company.values[idx]

                doc = {
                    "code": code,
                    "company": company,
                    "last_update": today
                }
                es.index(index=index, doc_type="_doc", id=code, body=doc)
                self.codes[code] = company
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print(f'[{tmnow}] #{idx+1:04d} REPLACE INTO company_info '\
                    f'VALUES ({code}, {company}, {today})')
        else:
            if results['aggregations']['max_date']['value'] != None: print('max_date is not None')
            if results['aggregations']['max_date']['value_as_string'] >= today: print('max_date is not less than today')

        print('')

    def update_daily_price(self, codes):
        
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""   
        """try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)
        print(f'pages_to_fetch[{pages_to_fetch}]')
        """
        
        for idx, code in enumerate(codes):
            df = self.read_naver(code, codes[code], self.pages_to_fetch)
            if df is None:
                continue
            #self.replace_into_index(df, idx, code, codes[code])
            #self.ies = Elasticsearch(['192.168.0.13','192.168.0.14','192.168.0.15'], port=9200, timeout=30, max_retries=10, retry_on_timeout=True)
            #self.replace_into_index(df, idx, code, codes[code])
            
            self.replace_into_index(df, idx, code, codes[code],Elasticsearch(['192.168.0.13','192.168.0.14','192.168.0.15'], port=9200, timeout=30, max_retries=10, retry_on_timeout=True))
            
    def read_naver(self, code, company, pages_to_fetch):
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환 """
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            html = BeautifulSoup(requests.get(url,
                headers={'User-agent': 'Mozilla/5.0'}).text, "lxml")
            pgrr = html.find("td", class_="pgRR")
            if pgrr is None:
                return None
            s = str(pgrr.a["href"]).split('=')
            lastpage = s[-1]
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                df = df.append(pd.read_html(requests.get(pg_url,
                    headers={'User-agent': 'Mozilla/5.0'}).text)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.
                    format(tmnow, company, code, page, pages), end="\r")
            df = df.rename(columns={'날짜':'date','종가':'close','전일비':'diff'
                ,'시가':'open','고가':'high','저가':'low','거래량':'volume'})
            df['date'] = df['date'].str.replace('.', '-')
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close',
                'diff', 'open', 'high', 'low', 'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
        except Exception as e:
            print('Exception occured :', str(e))
            return None
        return df
    
    def replace_into_index(self, df, num, code, company, ies):
        """네이버에서 읽어온 주식 시세를 DB에 REPLACE """
        index = 'daily_price'

        df = pd.DataFrame(data = {'code': code,
                                  'date': df['date'],
                                  'close': df['close'],
                                  'diff': df['diff'],
                                  'high': df['high'],
                                  'low':  df['low'],
                                  'open': df['open'],
                                  'volume': df['volume'],
                                  '_id': code + '-' + df['date']})
        documents = df.to_dict(orient='records')
                                  
        #print(documents)
        #bulk(ies, documents, index = index, doc_type='_doc', raise_on_error=True)
        bulk(ies, documents, index = index, raise_on_error=True)
        """
        for r in df.itertuples():
            doc = {
                "code": code,
                "date": r.date,
                "close": r.close,
                "diff": r.diff,
                "high": r.high,
                "low": r.low,
                "open": r.open,
                "volume": r.volume
            }
            ies.index(index=index, doc_type="_doc", id=code + '-' + r.date, body=doc)

        print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_'\
                'price [OK]'.format(datetime.now().strftime('%Y-%m-%d'\
                ' %H:%M'), num+1, company, code, len(df)))
        """    

    def split_codes_equally(self, chunks=10):
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


    def read_krx_code(self):
        """KRX로 부터 상장법인 목록 파일을 읽어와서 데이터프레임으로 변환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
              'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx        
    """"
    def cal(self, nb):
        print(nb)
        return nb * self.vl
    """
    def cal(self, sub_codes_list):
        print(sub_codes_list)
        
    """
    def run(self, dt):
        t = Pool(processes=4)
        rs = t.map(self.cal, dt)
        t.close()
        return t
    """
    """
    def run(self, sub_codes_list):
            pool = Pool(processes=10)
            pool.map(self.cal, sub_codes_list)
            pool.close()
            pool.join
            """
    def run(self, sub_codes_list):
            pool = Pool(processes=10)
            pool.map(self.update_daily_price, sub_codes_list)
            pool.close()
            pool.join

    def delete_index(self, index):
        es.indices.delete(index=index, ignore=[400, 404])

    def delete_config(self, file):
        if os.path.isfile(file):
            os.remove(file)
        return 'okay'

    def reset(self,pages_to_fetch):
        print('reset')
        self.delete_index('daily_price')
        self.delete_index('company_info')
        self.delete_config('config.json')
        self.codes = dict()

        index='company_info'

        with open('comp_mapping.json', 'r') as f:
            mapping = json.load(f)
            es.indices.create(index=index, body=mapping)
        
        index='daily_price'

        with open('price_mapping.json', 'r') as f:
            mapping = json.load(f)
            es.indices.create(index=index, body=mapping)

        self.pages_to_fetch = pages_to_fetch


    def setPage(self, pages_to_fetch): 
        with open('config.json', 'w') as out_file:
            self.pages_to_fetch = pages_to_fetch
            config = {'pages_to_fetch': pages_to_fetch}
            json.dump(config, out_file)

def isFirst(): 
    try:
        with open('config.json', 'r') as in_file:
            return False
    except FileNotFoundError:
        with open('config.json', 'w') as out_file:
            return True

            
if __name__ == '__main__':
    
    #시간 측정
    start_time = time.time()

    # 처음 실행인 확인

    a = A(2)
    if isFirst():
        print('first trial')
        a.reset(100)
        codes = a.update_comp_info()
        sub_codes_list = a.split_codes_equally(10)
        args = []
    
        print(f'sub_codes_list[{len(sub_codes_list)}]')
        #a.run(list(range(10)))
        a.run(sub_codes_list)
    else:
        print('Not first trial')
        a.setPage(1)
        codes = a.update_comp_info()
        sub_codes_list = a.split_codes_equally(10)
        args = []
    
        print(f'sub_codes_list[{len(sub_codes_list)}]')
        #a.run(list(range(10)))
        a.run(sub_codes_list)
        
    a.setPage(1)

    elapsed_time = time.time() - start_time
    #time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    print(f'elapsed time : [{time.strftime("%H:%M:%S", time.gmtime(elapsed_time))}]')
        
