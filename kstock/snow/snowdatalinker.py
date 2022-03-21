from kstock.ifdatalink import *
import snowflake.connector



class SnowflakeDataLinker(DataLinker):
    def __init__(self):


        # company_info index가 없을 경우 생성한다.
        if es.indices.exists(index=self.company_index_name) == False:
            self.logging.logger.debug('create company_info index')

            data = pkgutil.get_data(__name__, 'templates/comp_mapping.json')
            mapping = json.loads(data.decode())
            es.indices.create(index=self.company_index_name, body=mapping)
        else:
            self.logging.logger.debug('index for company_info exists ')

        # daily_price index가 없을 경우 생성한다.
        if es.indices.exists(index=self.price_index_name) == False:
            self.logging.logger.debug('create daily_price index')

            data = pkgutil.get_data(__name__, 'templates/price_mapping.json')
            mapping = json.loads(data.decode())
            es.indices.create(index=self.price_index_name, body=mapping)
        else:
            self.logging.logger.debug('index for daily_price exists ')

        self.codes = dict()

    def update_comp_info(self, target_url):
        pass


    def read_krx_code_api(self, source_url):
        pass


    def update_daily_price(self, codes, target_url):
        pass


    def read_naver(self, code, company, target_ulr):
        pass