'''
This is a module for interfaces
to be implemented by various data source classes.
'''
from abc import *

class DataLinker(metaclass=ABCMeta):
    @abstractmethod
    def update_comp_info(self, target_url):
        raise NotImplemented

    @abstractmethod
    def read_krx_code_api(self, source_url):
        raise NotImplemented

    @abstractmethod
    def update_daily_price(self, codes, pages_to_fetch=1):
        raise NotImplemented

    @abstractmethod
    def read_naver(self, code, company, pages_to_fetch=1):
        raise NotImplemented



