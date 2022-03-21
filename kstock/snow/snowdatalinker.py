import snowflake.connector
from kstock.ifdatalink import *

import pandas as pd
from datetime import date

from bs4 import BeautifulSoup

from itertools import repeat

import requests

from multiprocessing import Pool
import multiprocessing as mp

import sys

import sqlalchemy

con  = None

class SnowflakeDataLinker(DataLinker):
    def __init__(self):
        global con

        con = snowflake.connector.connect(
            user='kstocker',
            password='Park74055!',
            account='sfcsupport2',
            warehouse='YHP_6X_LARGE'
        )

        self.codes = dict()

        # company_info table이 없을 경우 어떻겍 할까요?
        cur = con.cursor()
        try:

            query = "select table_name from kstock.information_schema.tables " \
                    "where " \
                    "table_catalog = 'KSTOCK' " \
                    "and  table_schema = 'COMPANYSCH' " \
                    "and table_name = 'COMPANYINFO';"

            if cur.execute(query).rowcount == 0:
                raise StockError('COMPANYINFO does not exists!!!')
            print('COMPANYINFO ok')

            query = "select table_name from kstock.information_schema.tables " \
                    "where " \
                    "table_catalog = 'KSTOCK' " \
                    "and table_schema = 'PRICESCH' " \
                    "and table_name = 'DAILYPRICE';"

            if cur.execute(query).rowcount == 0:
                raise StockError('DAILYPRICE does not exists!!!')
            print('DAILYPRICE ok')

        # except NoTableError as nte:
        #     print(nte)
        finally:
            print('close cur')
            cur.close()

        # if es.indices.exists(index=self.company_index_name) == False:
        #     self.logging.logger.debug('create company_info index')
        #
        #     data = pkgutil.get_data(__name__, 'templates/comp_mapping.json')
        #     mapping = json.loads(data.decode())
        #     es.indices.create(index=self.company_index_name, body=mapping)
        # else:
        #     self.logging.logger.debug('index for company_info exists ')
        #
        # # daily_price index가 없을 경우 생성한다.
        # if es.indices.exists(index=self.price_index_name) == False:
        #     self.logging.logger.debug('create daily_price index')
        #
        #     data = pkgutil.get_data(__name__, 'templates/price_mapping.json')
        #     mapping = json.loads(data.decode())
        #     es.indices.create(index=self.price_index_name, body=mapping)
        # else:
        #     self.logging.logger.debug('index for daily_price exists ')
        #
        # self.codes = dict()

    def update_comp_info(self, target_url):
        """종목코드를 companyinfo table에 업데이트한 후 딕셔너리에 저장"""
        # table = 'companyinfo'
        query = "select * from kstock.companysch.companyinfo;"
        cur = con.cursor()

        try:
            results = cur.execute(query)

            max_last_update = None
            for (code, company, market_code, last_update) in results:
                if max_last_update is None or max_last_update < last_update:
                    max_last_update = last_update
                self.codes[code] = company

            if max_last_update is None or max_last_update < date.today():
                print('update max:{0}, todayd:{1}'.format(max_last_update, date.today()))

                krx = self.read_krx_code_api('aaaaaa')

                if len(krx) <= 0:
                    raise StockError('krx no data')

                today = date.today().strftime('%Y-%m-%d')

                using_query = "SELECT column1 as code, column2 as company, column3 as market_code, column4 as last_update from ( values"

                # update codes and  make using query
                for idx in range(len(krx)):
                    self.codes[krx.code.values[idx]] = krx.company.values[idx]
                    using_query += "('" + krx.code.values[idx] + "','" + krx.company.values[idx] + "','" + \
                                   krx.market_code.values[idx] + "','" + today + "'::date),"
                using_query = using_query[:-1]
                using_query += ")"

                query = "merge into kstock.companysch.companyinfo t1 using (" \
                        + using_query \
                        + ") t2 on t1.code = t2.code " \
                          "when matched then update set t1.company = t2.company, t1.market_code = t2.market_code, " \
                          "t1.last_update = t2. last_update " \
                          "when not matched then insert values (t2.code, t2.company, t2.market_code, t2.last_update);"

                mergeres = cur.execute(query)
                # print(query)
                for rec in mergeres:
                    print('insert : %s, update : %s, krx : %s ' % (rec[0], rec[1], len(krx)))

            else:
                print('pass max:{0}, todayd:{1}'.format(max_last_update, date.today()))

        finally:
            cur.close()


    def update_daily_price(self, codes, pages_to_fetch=1):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        firstCode = next(iter(codes))
        print('first code :{0}, pages_to_f:{1}'.format(firstCode, pages_to_fetch))

        icon = snowflake.connector.connect(
            user='kstocker',
            password='Park74055!',
            account='sfcsupport2',
            warehouse='YHP_6X_LARGE'
        )

        totalcount = 0
        using_query_list = ''
        for idx, code in enumerate(codes):
            df = self.read_naver(code, codes[code], pages_to_fetch)

            if df is None or df.empty:
                continue
            using_query = "select column1 as code, column2 as \"date\", column3 as open, column4 as high, column5 as low, column6 as close, column7 as diff, column8 as volume from ( values"
            for iidx, row in df.iterrows():
                using_query +=  "('" + code + "','" + row['date'] + "'::date," + str(row['open']) +"," + str(row['high']) +"," + str(row['low'])+"," + str(row['close'])+"," + str(row['diff'])+"," + str(row['volume']) + "),"
                if iidx == 1 : totalcount += 1

            using_query = using_query[:-1]

            if idx < len(codes) -1 :
                using_query_list +=  using_query + ") union all "
            else:
                using_query_list +=  using_query + ") "

        query = "merge into kstock.pricesch.dailyprice t1 using (" \
                + using_query_list \
                + ")t2 on t1.code = t2.code and t1.\"date\" = t2.\"date\" when matched then update set t1.open = t2.open, t1.high = t2.high, t1.low = t2.low, t1.close = t2.close, t1.diff = t2.diff, t1.volume = t2.volume when not matched then insert values (t2.code, t2.\"date\", t2.open, t2.high, t2.low, t2.close, t2.diff, t2.volume);"


        print('finish :{0}, totlacount :{1},  query:{2}'.format(firstCode, totalcount, 'refer to history'))

        if totalcount > 0 :
            icur = icon.cursor()
            try:
                mergeres = icur.execute(query)
            finally:
                icur.close()


    def read_krx_code_api(self, source_url):
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

    # def update_daily_price(self, codes, target_url):
    #     pass
    #     # def update_daily_price(self, codes):



    def read_naver(self, code, company, pages_to_fetch=1):
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환 """
        # count=3000에서 3000은 과거 3,000 영업일간의 데이터를 의미. 사용자가 조절 가능

        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count={pages_to_fetch + 1}&requestType=0"

        get_result = requests.get(url)
        bs_obj = BeautifulSoup(get_result.content, "html.parser")

        # information
        inf = bs_obj.select('item')
        columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df_inf = pd.DataFrame([], columns=columns, index=range(len(inf)))

        for i in range(len(inf)):
            df_inf.iloc[i] = str(inf[i]['data']).split('|')

        df_inf['date'] = pd.to_datetime(df_inf['date']).dt.strftime('%Y-%m-%d')
        # df_inf['date'] = df_inf['date'].dt.strftime('%Y-%m-%d')
        # print(df_inf)

        df_inf[['close', 'open', 'high', 'low', 'volume']] = df_inf[['close', 'open', 'high', 'low', 'volume']].astype(
            int)
        dff_inf = df_inf['close'].diff().abs()
        df_inf['diff'] = dff_inf
        df_inf = df_inf.dropna()
        df_inf = df_inf[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]

        return df_inf

    # multi thread 처리를 위한 함수
    def run(self, processNum=1, pages_to_fetch=1):
        print('run!!!!')
        sub_codes_list = self.split_codes_equally(processNum)

        pool = Pool(processes=processNum)
        # pool.map(self.update_daily_price, sub_codes_list)
        # pool.map(self.update_daily_price, zip(sub_codes_list, repeat(pages_to_fetch)))
        pool.starmap(self.update_daily_price, zip(sub_codes_list, repeat(pages_to_fetch)))

        pool.close()

        pool.join

    def split_codes_equally(self, chunks=1):
        print('split')
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


    def write_file_send(self, filename, target_URL):
        pass

class StockError(Exception):
    def __ini__(self, msg):
        super().__init__(msg)


# comment : https://community.snowflake.com/s/question/0D50Z00008759PiSAI/error-maximum-number-of-expressions-in-a-list-exceeded-expected-at-most-16384-got-29933
# maximum number of expressions in a list exceeded, expected at most 16,384, got 250,088