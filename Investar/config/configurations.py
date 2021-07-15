'''
class HostInfo(object):

    HostUrls = {
        'hostGubun': {
            '1': ['192.168.0.13','192.168.0.14','192.168.0.15'],
            '2': ['jjundol.iptime.org']
        }
    }
'''
def getESHost(gubun):
    host_dict={
        'internal': ['192.168.0.16','192.168.0.14','192.168.0.15'],

        'external': ['jjundol.iptime.org'],

        'k8s': ['192.168.0.38', '192.168.0.39']

        }
    return host_dict[gubun]

def getESPort(gubun):
    port_dict = {
        'internal': 9200,

        'external': 9200,

        'k8s': 30245

    }
    return port_dict[gubun]

def getIndexName(gubun):
    index_dict={
        'company_index_name' : 'company_info_01',
        'price_index_name' : 'daily_price_01'
        }

    return index_dict[gubun]