import pandas as pd
import mplfinance as mpf
from Investar import Analyzer

mk = Analyzer.MarketDB()
df = mk.get_daily_price('엔씨소프트', '2019-01-01')
df.index = pd.to_datetime(df.date)
df = df[['open', 'high', 'low', 'close', 'volume']]

ema60 = df.close.ewm(span=60).mean()
ema130 = df.close.ewm(span=130).mean() 
macd = ema60 - ema130
signal = macd.ewm(span=45).mean() 
macdhist = macd - signal

ndays_high = df.high.rolling(window=14, min_periods=1).max()      # ①
ndays_low = df.low.rolling(window=14, min_periods=1).min()        # ②
fast_k = (df.close - ndays_low) / (ndays_high - ndays_low) * 100  # ③
slow_d= fast_k.rolling(window=3).mean()                           # ④
#df = df.assign(fast_k=fast_k, slow_d=slow_d).dropna()             # ⑤

apds = [
        mpf.make_addplot(ema130, color='c'),
        mpf.make_addplot(fast_k, panel=1, color='c'),
        mpf.make_addplot(slow_d, panel=1, color='k')
    ]

mc = mpf.make_marketcolors(up='r', down='b', inherit=True) 
stl = mpf.make_mpf_style(marketcolors=mc) 
mpf.plot(df, title='Triple Screen Trading - Second Screen (NCSOFT)', type='candle',
    addplot=apds, figsize=(9,7), panel_ratios=(1,1), style=stl) 

'''
plt.figure(figsize=(9, 7))
p1 = plt.subplot(2, 1, 1)
plt.title('Triple Screen Trading - Second Screen (NCSOFT)')
plt.grid(True)
candlestick_ohlc(p1, ohlc.values, width=.6, colorup='red', colordown='blue')
p1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.plot(df.number, df['ema130'], color='c', label='EMA130')
plt.legend(loc='best')
p1 = plt.subplot(2, 1, 2)
plt.grid(True)
p1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.plot(df.number, df['fast_k'], color='c', label='%K')
plt.plot(df.number, df['slow_d'], color='k', label='%D')
plt.yticks([0, 20, 80, 100]) # ⑥
plt.legend(loc='best')
plt.show()'''
