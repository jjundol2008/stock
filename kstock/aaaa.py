import sys
from multiprocessing import Pool



class A(object):

    def __init__(self, vl):
        self.vl = vl

    def parallel_call(self,params):  # a helper for calling 'remote' instances
        cls = getattr(sys.modules[__name__], params[0])  # get our class type
        instance = cls.__new__(cls)  # create a new instance without invoking __init__
        instance.__dict__ = params[1]  # apply the passed state to the new instance
        method = getattr(instance, params[2])  # get the requested method
        args = params[3] if isinstance(params[3], (list, tuple)) else [params[3]]
        return method(*args)  # expand arguments, call our method and return the result

    def cal(self, nb):
        return nb * self.vl

    def run(self, dt):
        t = Pool(processes=4)
        rs = t.map(self.parallel_call, self.prepare_call("cal", dt))
        t.close()
        return rs

    def prepare_call(self, name, args):  # creates a 'remote call' package for each argument
        for arg in args:
            yield [self.__class__.__name__, self.__dict__, name, arg]

