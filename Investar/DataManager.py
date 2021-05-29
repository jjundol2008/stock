import pandas as pd
from bs4 import BeautifulSoup
import requests, calendar, time, json
from datetime import datetime
from threading import Timer
from elasticsearch import Elasticsearch
from multiprocessing import Process, Queue
import pkgutil

class ESManager:

    def __init__(self):
        """생성자: ES 연결 및 종목코드 딕셔너리 생성 """
        print('init start')

        self.es = Elasticsearch(['192.168.0.13','192.168.0.14','192.168.0.15'], port=9200, timeout=30, max_retries=10, retry_on_timeout=True)

        """ company_info index가 없을 경우 생성한다. """
        if self.es.indices.exists(index='company_info') == False:
            print('create company_info index')

            index='company_info'

            with open('comp_mapping.json', 'r') as f:
                mapping = json.load(f)

            self.es.indices.create(index=index, body=mapping)
        else:
            print('company_info exists ')

        """ daily_price index가 없을 경우 생성한다. """
        if self.es.indices.exists(index='daily_price') == False:
            print('create daily_price index')

            index='daily_price'

            with open('price_mapping.json', 'r') as f:
                mapping = json.load(f)

            self.es.indices.create(index=index, body=mapping)
        else:
            print('daily_price exists ')

        self.codes = dict()
        

    def __del__(self):
        """소멸자: ES 연결 해제"""
        print('end')
        

    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price index 업데이트 """
        self.update_comp_info()

        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)


    def update_daily_price(self, codes, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        print('pages_to_fetch', pages_to_fetch)

        for idx, code in enumerate(codes):
            df = self.read_naver(code, codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_index(df, idx, code, codes[code])

    """   
    def update_daily_price(self, pages_to_fetch):
        #KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트
        print('pages_to_fetch', pages_to_fetch)

        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_index(df, idx, code, self.codes[code])
        
        #주요 4개사 만 가져올때
        #for idx, code in enumerate(self.codes):
        #    if self.codes[code] in ('삼성전자', 'SK하이닉스','현대자동차','NAVER'):
        #        df = self.read_naver(code, self.codes[code], pages_to_fetch)
        #        if df is None:
        #            continue
        #        self.replace_into_index(df, idx, code, self.codes[code])
    """    
   
    
    def replace_into_index(self, df, num, code, company):
        """네이버에서 읽어온 주식 시세를 DB에 REPLACE """
        index = 'daily_price'

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
            self.es.index(index=index, doc_type="_doc", id=code + '-' + r.date, body=doc)

        print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_'\
                'price [OK]'.format(datetime.now().strftime('%Y-%m-%d'\
                ' %H:%M'), num+1, company, code, len(df)))


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


""" Global Function """
ges = Elasticsearch(['192.168.0.13','192.168.0.14','192.168.0.15'], port=9200)
gcodes = dict()

def getPageToFetch():
    try:
        with open('config.json', 'r') as in_file:
            config = json.load(in_file)
            pages_to_fetch = config['pages_to_fetch']
    except FileNotFoundError:
        with open('config.json', 'w') as out_file:
            pages_to_fetch = 100
            config = {'pages_to_fetch': 1}
            json.dump(config, out_file)
    return pages_to_fetch

def split_dict_equally(input_dict, chunks=10):
    "Splits dict by kes. Returna a list of dictionaries"
    # prep with empty dicts
    return_list = [dict() for idx in range(chunks)]
    idx = 0
    for k,v in input_dict.items():
        return_list[idx][k] = v
        if idx < chunks-1: # indexes start at 0
            idx += 1
        else:
            idx = 0
    return return_list


def update_comp_info():
    """종목코드를 company_info index에 업데이트한 후 딕셔너리에 저장"""
    """ company_info index가 없을 경우 생성한다. """
    if ges.indices.exists(index='company_info') == False:
        print('create company_info index')

        index='company_info'

        data = pkgutil.get_data(__name__, 'templates/comp_mapping.json')
        mapping = json.loads(data.decode())
        ges.indices.create(index=index, body=mapping)
    else:
         print('company_info exists ')

    """ daily_price index가 없을 경우 생성한다. """
    if ges.indices.exists(index='daily_price') == False:
        print('create daily_price index')

        index='daily_price'

        data = pkgutil.get_data(__name__, 'templates/price_mapping.json')
        
        mapping = json.loads(data.decode())
        ges.indices.create(index=index, body=mapping)
    else:
        print('daily_price exists ')        
            
    index = 'company_info'
    body = {
      "query": {
        "match_all": {}
      },
      "size": 10000
    }

    results = ges.search(index=index, body=body)

    for result in results['hits']['hits']:
        gcodes[result['_source']['code']] = result['_source']['company']

    message = f'codes\'s size is {len(gcodes)}'
    print(message)

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
    results = ges.search(index=index, body=body)

    if results['aggregations']['max_date']['value'] == None or results['aggregations']['max_date']['value_as_string'] < today:
        krx = read_krx_code()
        for idx in range(len(krx)):
            code = krx.code.values[idx]
            company = krx.company.values[idx]

            doc = {
                "code": code,
                "company": company,
                "last_update": today
            }
            ges.index(index=index, doc_type="_doc", id=code, body=doc)
            gcodes[code] = company
            tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
            print('[{tmnow}] #{idx+1:04d} REPLACE INTO company_info '\
                'VALUES ({code}, {company}, {today})')
    else:
        if results['aggregations']['max_date']['value'] != None: print('max_date is not None')
        if results['aggregations']['max_date']['value_as_string'] >= today: print('max_date is not less than today')

    return gcodes


def read_krx_code():
        """KRX로 부터 상장법인 목록 파일을 읽어와서 데이터프레임으로 변환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
              'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx
    

if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.update_comp_info()
    pages_to_fetch = dbu.getPageToFetch()
    sub_codes_list = dbu.split_dict_equally(dbu.codes, 6)

    result = Queue()
    checkSet = set()
    procs = []
    for idx in range(len(sub_codes_list)):
        print(f'idx[{idx}], sub_codes[{len(sub_codes_list[idx])}]')
        proc = Process(target=update_daily_price, args=(sub_codes_list[idx], pages_to_fetch, idx, result))
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
    # Timeout https://somjang.tistory.com/entry/Elasticsearch-ConnectionTimeout-caused-by-ReadTimeoutError-feat-bulk-API-python
