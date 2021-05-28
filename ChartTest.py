import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from Investar import Analyzer



mk = Analyzer.MarketDB()
stocks = ['삼성전자', 'SK하이닉스', '현대자동차','NAVER']
df = pd.DataFrame()
for s in stocks:
    df[s] = mk.get_daily_price(s, '2016-01-04', '2018-04-27')['close']

daily_ret = df.pct_change()
annual_ret = daily_ret.mean() * 252
daily_cov = daily_ret.cov()
annual_con = daily_cov * 252

print(daily_ret)
