import sys

from Investar.dataLinker import *
import  time
from Investar.logger import *

################### For Setting ########

PATHS = {'acer':'C:\\dev\\python\\stock\\', 'home_server':'/home/jjundol/dev/stock/', 'K8S':'asdfasdf' }

#PATH = {}'C:\\dev\\python\\stock\\'
PATH = PATHS['acer']
logging = Logging()

def isFirst():
    try:
        with open(PATH + 'initial', 'r') as in_file:
            return False
    except FileNotFoundError:
            return True

def setInitial():
    with open(PATH + 'initial', 'w') as out_file:
        config = {'pages_to_fetch': 'helloworld'}
        json.dump(config, out_file)

if __name__ == '__main__':

    #시간 측정
    start_time = time.time()

    #dl = DataLinKer('internal')
    dl = DataLinKer('external')
    #dl = DataLinKer('k8s')
    dl.update_comp_info()

        #dl.setPagesToFetch(1)
    if isFirst():
        logging.logger.debug('First Execution')
        dl.run(10, 1200) # 10개로 돌린다. 1200일치 데이터 가지고 온다.

    else:
        logging.logger.debug('Not First Execution')
        dl.run(10, 5) # 10개로 돌린다. 5일치 데이터 가지고 온다.

    setInitial()

    elapsed_time = time.time() - start_time
    logging.logger.debug(f'elapsed time : [{time.strftime("%H:%M:%S", time.gmtime(elapsed_time))}]')





#
# codes = dl.getCodes()
#
# print(dl.update_daily_price(codes))
