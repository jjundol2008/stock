from datetime import datetime, timedelta
from Investar import Analyzer
from multiprocessing import Process, Pool, Queue


# 한글 폰트 사용을 위해서 세팅
from matplotlib import font_manager, rc

'''
font_path = "C:/Windows/Fonts/NGULIM.TTF"
font = font_manager.FontProperties(fname=font_path).get_name()
rc('font', family=font)
'''

def get_portfolio(codes):

    mk = Analyzer.MarketDB()

    target_date = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
    buy_stock_list = {}
    sell_stock_list = {}

    for code in codes:
        #print('code:', code, 'company name:', mk.codes[code])
        try:
            df = mk.get_daily_price(code)

            ema60 = df.close.ewm(span=60).mean()
            ema130 = df.close.ewm(span=130).mean()
            macd = ema60 - ema130
            signal = macd.ewm(span=45).mean()
            macdhist = macd - signal
            df = df.assign(ema130=ema130, ema60=ema60, macd=macd, signal=signal, macdhist=macdhist).dropna()

            ndays_high = df.high.rolling(window=14, min_periods=1).max()
            ndays_low = df.low.rolling(window=14, min_periods=1).min()

            fast_k = (df.close - ndays_low) / (ndays_high - ndays_low) * 100
            slow_d = fast_k.rolling(window=3).mean()
            df = df.assign(fast_k=fast_k, slow_d=slow_d).dropna()

            for i in range(1, len(df.close)):
                if df.ema130.values[i - 1] < df.ema130.values[i] and df.slow_d.values[i - 1] >= 20 and df.slow_d.values[i] < 20:
                    if target_date == df.date.values[i]:
                        # print('매수 : ',df.date.values[i])
                        buy_stock_list.update({code: {"종목명": mk.codes[code], "현재가": df.close.values[i]}})
                        #bq.put({code: {"종목명": mk.codes[code], "현재가": df.close.values[i]}})
                elif df.ema130.values[i - 1] > df.ema130.values[i] and df.slow_d.values[i - 1] <= 80 and df.slow_d.values[i] > 80:
                    if target_date == df.date.values[i]:
                        # print('매도 : ', df.date.values[i])
                        sell_stock_list.update({code: {"종목명": mk.codes[code], "현재가": df.close.values[i]}})
                        #sq.put({code: {"종목명": mk.codes[code], "현재가": df.close.values[i]}})
        except Exception as e:
            print("예외가 발생했습니다.", e)
            continue

    #print(f'매수리스트 {buy_stock_list}')
    #print(f'매도리스트 {sell_stock_list}')
    return buy_stock_list

    '''
    f = open("files/condition_stock.txt", "a", encoding="utf8")
    for code in buy_stock_list:
        f.write("%s\t%s\t%s\n" % (code, buy_stock_list[code]['종목명'], buy_stock_list[code]['현재가']))
    f.close()
    '''


def split_codes_equally(codes, chunks=10):
    "Splits dict by kes. Returna a list of dictionaries"
    # prep with empty dicts
    return_list = [dict() for idx in range(chunks)]
    idx = 0
    for k,v in codes.items():
        return_list[idx][k] = v
        if idx < chunks-1: # indexes start at 0
            idx += 1
        else:
            idx = 0
    return return_list


def run(sub_codes_list):
    bq = Queue()
    sq = Queue()
    pool = Pool(processes=10)

    result = pool.map(get_portfolio, sub_codes_list)
    pool.close()
    pool.join

    if not result:
        print("no result")
    else:
        f = open("files/condition_stock.txt", "a", encoding="utf8")
        for codes in result:
            for code in codes:
                f.write("%s\t%s\t%s\n" % (code, codes[code]['종목명'], codes[code]['현재가']))
        f.close()

if __name__ == '__main__':
    mk = Analyzer.MarketDB()
    codes_lists = split_codes_equally(mk.codes,10)
    run(codes_lists)

    # the article and the keywords
    '''
    article = """The multiprocessing package also includes some APIs that are not in the threading module at all. For example, there is a neat Pool class that you can use to parallelize executing a function across multiple inputs."""
    keywords = ["threading", "package", "parallelize"]

    # construct the arguments for the search_worker; one keyword per worker but same article
    args = [(article, keyword) for keyword in keywords]
    print(args)
    '''