import pandas as pd
import pymysql
from datetime import datetime
from datetime import timedelta
import re
from elasticsearch import Elasticsearch
import elasticsearch.helpers

class MarketDB:
    def __init__(self):
        """생성자: ES 연결 및 종목코드 딕셔너리 생성 """
        print('init start')

        self.es = Elasticsearch(['192.168.0.13','192.168.0.14','192.168.0.15'], port=9200)
        
        self.codes = {}
        self.get_comp_info()
        
    def __del__(self):
        """소멸자:  연결 해제"""
        self.es.transport.close()
        
    def get_comp_info(self):
        """company_info index에서 읽어와서 codes에 저장"""
        index = 'company_info'
        body = {
          "query": {
            "match_all": {}
          }
        }

        results = elasticsearch.helpers.scan(self.es, query=body, scroll='10m', size=10000, index=index)
         
        for result in results:
            self.codes[result['_source']['code']] = result['_source']['company']
            
        print(f'codes\'s size is [{len(self.codes)}]')

    def get_daily_price(self, code, start_date=None, end_date=None):
        """KRX 종목의 일별 시세를 데이터프레임 형태로 반환
            - code       : KRX 종목코드('005930') 또는 상장기업명('삼성전자')
            - start_date : 조회 시작일('2020-01-01'), 미입력 시 1년 전 오늘
            - end_date   : 조회 종료일('2020-12-31'), 미입력 시 오늘 날짜
        """
        if start_date is None:
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to '{}'".format(start_date))
        else:
            start_lst = re.split('\D+', start_date)
            if start_lst[0] == '':
                start_lst = start_lst[1:]
            start_year = int(start_lst[0])
            start_month = int(start_lst[1])
            start_day = int(start_lst[2])
            if start_year < 1900 or start_year > 2200:
                print(f"ValueError: start_year({start_year:d}) is wrong.")
                return
            if start_month < 1 or start_month > 12:
                print(f"ValueError: start_month({start_month:d}) is wrong.")
                return
            if start_day < 1 or start_day > 31:
                print(f"ValueError: start_day({start_day:d}) is wrong.")
                return
            start_date=f"{start_year:04d}-{start_month:02d}-{start_day:02d}"

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            print("end_date is initialized to '{}'".format(end_date))
        else:
            end_lst = re.split('\D+', end_date)
            if end_lst[0] == '':
                end_lst = end_lst[1:] 
            end_year = int(end_lst[0])
            end_month = int(end_lst[1])
            end_day = int(end_lst[2])
            if end_year < 1800 or end_year > 2200:
                print(f"ValueError: end_year({end_year:d}) is wrong.")
                return
            if end_month < 1 or end_month > 12:
                print(f"ValueError: end_month({end_month:d}) is wrong.")
                return
            if end_day < 1 or end_day > 31:
                print(f"ValueError: end_day({end_day:d}) is wrong.")
                return
            end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"
       
        codes_keys = list(self.codes.keys())
        codes_values = list(self.codes.values())
        
        if code in codes_keys:
            pass
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print(f"ValueError: Code({code}) doesn't exist.")

        index = 'daily_price'
        
        body = {
            "query": {
                "bool": {
                    "must": [
                    {
                      "term":{ "code":code }
                    },
                    {
                      "range": {
                        "date": {
                          "gte": start_date,
                          "lte": end_date
                        }
                      }
                    }
                  ]
                }
            }
        }
        
        results = elasticsearch.helpers.scan(self.es, query=body, scroll='10m', size=10000, index=index)
        df = pd.DataFrame.from_dict([document['_source'] for document in results])

        if len(list(df)) == 0 :
            raise Exception('No Data')
        else:
            df.index = df['date']
            df = df.sort_index()
           
        return df

    def test_panda(self):
       
        index = 'daily_price'
        # 145개

        """doc = {
            "query": {
                "bool": {
                    "must": [
                    {
                      "term":{ "code":"023960" }
                    },
                    {
                      "range": {
                        "date": {
                          "gte": "2019-03-01",
                          "lt": "2021-10-01"
                        }
                      }
                    }
                  ]
                }
            }
        }"""
        doc = {
            "query": { "match_all" :{} }
        }
        results = elasticsearch.helpers.scan(self.es, query=doc, scroll='10m', size=10000, index=index)
        df = pd.DataFrame.from_dict([document['_source'] for document in results])
        print(len(df))
       # print(df)
    

if __name__ == '__main__':
    dbu = MarketDB()
    #dbu.get_daily_price('005930', '2019-03-01', '2021-10-01')
    #dbu.get_daily_price('삼성전자', '2019-09-30', '2019.10.4')
    # def get_daily_price(self, code, start_date=None, end_date=None):   



    """{
        "_index" : "daily_price",
        "_type" : "_doc",
        "_id" : "023960-2019-03-22",
        "_score" : 5.313463,
        "_source" : {
          "code" : "023960",
          "date" : "2019-03-22",
          "close" : 1740,
          "diff" : 10,
          "high" : 1770,
          "low" : 1720,
          "open" : 1770,
          "volume" : 26651
        }
      }"""

    
    # scrol API : https://devuna.tistory.com/50
    # https://yujuwon.tistory.com/entry/ELASTIC-SEARCH-full-scan-%ED%95%98%EA%B8%B0
    # https://stackoverflow.com/questions/47722238/python-elasticsearch-helpers-scan-example
    # 멀티 쓰레드, 멀티 프로세스 https://monkey3199.github.io/develop/python/2018/12/04/python-pararrel.html
    # 멀티 : https://pymotw.com/2/multiprocessing/basics.html
    # 멀티 : https://seing.tistory.com/92
    # split dictionary : https://gist.github.com/miloir/2196917
    
