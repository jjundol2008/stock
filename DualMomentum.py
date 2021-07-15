import pandas as pd
import pymysql
from datetime import datetime
from datetime import timedelta
from Investar import Analyzer
from elasticsearch import Elasticsearch
import requests, calendar, time, json

class DualMomentum:
    def __init__(self):
        """생성자: KRX 종목코드(codes)를 구하기 위한 MarkgetDB 객체 생성"""
        self.mk = Analyzer.MarketDB()
        self.es = Elasticsearch(['192.168.0.13','192.168.0.14','192.168.0.15'], port=9200, timeout=30, max_retries=10, retry_on_timeout=True)
    
    def get_rltv_momentum(self, start_date, end_date, stock_count):
        """특정 기간 동안 수익률이 제일 높았던 stock_count 개의 종목들 (상대 모멘텀)
            - start_date  : 상대 모멘텀을 구할 시작일자 ('2020-01-01')   
            - end_date    : 상대 모멘텀을 구할 종료일자 ('2020-12-31')
            - stock_count : 상대 모멘텀을 구할 종목수
        """              
        
        # 사용자가 입력한 시작일자를 DB에서 조회되는 일자로 보정 
        index ='daily_price'
        body = {
                  "query": {
                    "range":{
                        "date":{
                          "lte":start_date
                        }
                    }
                  },"aggs":{
                      "max_start":{
                        "max": {
                          "field":"date",
                          "format": "yyyy-MM-dd"
                      }
                    }
                  },"size":0
                }
        
        results = self.es.search(index=index, body=body)

        if results['aggregations']['max_start']['value'] == None:
            print (f"start_date : {body} -> returned None")
            return

        start_date = results['aggregations']['max_start']['value_as_string']
        print(start_date)


        # 사용자가 입력한 종료일자를 DB에서 조회되는 일자로 보정
        index ='daily_price'
        body = {
                  "query": {
                    "range":{
                        "date":{
                          "lte":end_date
                        }
                    }
                  },"aggs":{
                      "max_end":{
                        "max": {
                          "field":"date",
                          "format": "yyyy-MM-dd"
                      }
                    }
                  },"size":0
                }
        
        results = self.es.search(index=index, body=body)

        if results['aggregations']['max_end']['value'] == None:
            print (f"end_date : {body} -> returned None")
            return

        end_date = results['aggregations']['max_end']['value_as_string']
        print(end_date)



        # KRX 종목별 수익률을 구해서 2차원 리스트 형태로 추가
        rows = []
        columns = ['code', 'company', 'old_price', 'new_price', 'returns']
        
        for _, code in enumerate(self.mk.codes):
            body = {
                      "query":{
                        "bool":{
                          "must":[
                            {
                              "term":{
                                "code":code
                              }
                            },
                               {
                                "terms":{
                                  "date":[start_date, end_date]
                                }
                              }
                            ]
                            
                        }
                      },
                      "_source": {
                          "includes": ["date","close"]
                      }    
                    }
            
            results = self.es.search(index=index, body=body)

            old_price = int()
            new_price = int()
            
            if results['hits']['total']['value'] != 2:
                continue
            else:
                for result in results['hits']['hits']:
                    if result['_source']['date'] == start_date:
                        old_price = result['_source']['close']
                    elif result['_source']['date'] == end_date:
                        new_price = result['_source']['close']
                    else:
                        print('What the hell!')
                        
            if old_price == 0 or new_price == 0 :
                print(f'Some problems conde[{code}], start_date[{start_date}], end_date[{end_date}]')

            returns = (new_price/old_price - 1) * 100
            rows.append([code, self.mk.codes[code], old_price, new_price, 
                returns])
                                         
        # 상대 모멘텀 데이터프레임을 생성한 후 수익률순으로 출력
        df = pd.DataFrame(rows, columns=columns)
        df = df[['code', 'company', 'old_price', 'new_price', 'returns']]
        df = df.sort_values(by='returns', ascending=False)
        df = df.head(stock_count)
        df.index = pd.Index(range(stock_count))
   
        print(df)
        print(f"\nRelative momentum ({start_date} ~ {end_date}) : "\
            f"{df['returns'].mean():.2f}% \n")
        return df

    def get_abs_momentum(self, rltv_momentum, start_date, end_date):
        """특정 기간 동안 상대 모멘텀에 투자했을 때의 평균 수익률 (절대 모멘텀)
            - rltv_momentum : get_rltv_momentum() 함수의 리턴값 (상대 모멘텀)
            - start_date    : 절대 모멘텀을 구할 매수일 ('2020-01-01')   
            - end_date      : 절대 모멘텀을 구할 매도일 ('2020-12-31')
        """
        stockList = list(rltv_momentum['code'])        

        # 사용자가 입력한 매수일을 DB에서 조회되는 일자로 변경 
        index ='daily_price'
        body = {
                  "query": {
                    "range":{
                        "date":{
                          "lte":start_date
                        }
                    }
                  },"aggs":{
                      "max_start":{
                        "max": {
                          "field":"date",
                          "format": "yyyy-MM-dd"
                      }
                    }
                  },"size":0
                }
        
        results = self.es.search(index=index, body=body)

        if results['aggregations']['max_start']['value'] == None:
            print (f"start_date : {body} -> returned None")
            return

        start_date = results['aggregations']['max_start']['value_as_string']


        # 사용자가 입력한 매도일을 DB에서 조회되는 일자로 변경 
        index ='daily_price'
        body = {
                  "query": {
                    "range":{
                        "date":{
                          "lte":end_date
                        }
                    }
                  },"aggs":{
                      "max_end":{
                        "max": {
                          "field":"date",
                          "format": "yyyy-MM-dd"
                      }
                    }
                  },"size":0
                }
        
        results = self.es.search(index=index, body=body)

        if results['aggregations']['max_end']['value'] == None:
            print (f"end_date : {body} -> returned None")
            return

        end_date = results['aggregations']['max_end']['value_as_string']

        # 상대 모멘텀의 종목별 수익률을 구해서 2차원 리스트 형태로 추가
        rows = []
        columns = ['code', 'company', 'old_price', 'new_price', 'returns']
        for _, code in enumerate(stockList):            
            body = {
                      "query":{
                        "bool":{
                          "must":[
                            {
                              "term":{
                                "code":code
                              }
                            },
                               {
                                "terms":{
                                  "date":[start_date, end_date]
                                }
                              }
                            ]
                            
                        }
                      },
                      "_source": {
                          "includes": ["date","close"]
                      }    
                    }
            
            results = self.es.search(index=index, body=body)

            old_price = int()
            new_price = int()
            
            if results['hits']['total']['value'] != 2:
                continue
            else:
                for result in results['hits']['hits']:
                    if result['_source']['date'] == start_date:
                        old_price = result['_source']['close']
                    elif result['_source']['date'] == end_date:
                        new_price = result['_source']['close']
                    else:
                        print('What the hell!')
                        
            if old_price == 0 or new_price == 0 :
                print(f'Some problems conde[{code}], start_date[{start_date}], end_date[{end_date}]')

            returns = (new_price/old_price - 1) * 100
            rows.append([code, self.mk.codes[code], old_price, new_price, 
                returns])


        # 절대 모멘텀 데이터프레임을 생성한 후 수익률순으로 출력
        df = pd.DataFrame(rows, columns=columns)
        df = df[['code', 'company', 'old_price', 'new_price', 'returns']]
        df = df.sort_values(by='returns', ascending=False)
      
        print(df)
        print(f"\nAbasolute momentum ({start_date} ~ {end_date}) : "\
            f"{df['returns'].mean():.2f}%")
        return

start_time = time.time()
test = DualMomentum()
rm = test.get_rltv_momentum('2020-12-08', '2021-03-08', 300)
am = test.get_abs_momentum(rm, '2021-03-08', '2021-06-08')
elapsed_time = time.time() - start_time
print(f'elapsed time : [{time.strftime("%H:%M:%S", time.gmtime(elapsed_time))}]')            
            
