import sys

from kstock.snow.snowdatalinker import SnowflakeDataLinker
import  time
from kstock.aaaa import A



if __name__ == '__main__':

    #시간 측정
    start_time = time.time()

    dl = SnowflakeDataLinker()

    dl.update_comp_info('aaa')

    # dl.run(10, 1200)  # 10개로 돌린다. 1200일치 데이터 가지고 온다.
    dl.run(10, 1200)  # 10개로 돌린다. 5 일치 데이터 가지고 온다.

    elapsed_time = time.time() - start_time

    print((f'elapsed time : [{time.strftime("%H:%M:%S", time.gmtime(elapsed_time))}]'))
    # print(dl.run2(list(range(10))))

    #result = dl.run2(list(range(10)))
    #print(result)
    #
    # a = A(2)
    # print(a.run(list(range(10))))