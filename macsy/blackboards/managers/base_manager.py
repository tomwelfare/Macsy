import inspect
from bson.codec_options import DEFAULT_CODEC_OPTIONS
codec_options = DEFAULT_CODEC_OPTIONS.with_options(unicode_decode_error_handler='ignore')

class BaseManager():

    def __init__(self, parent, suffix):
        self.checkCaller()
        self._parent = parent
        self._collection = self._parent._db.get_collection(self._parent._name + suffix, codec_options=codec_options)

    def checkCaller(self):
        from macsy.blackboards import blackboard, date_based_blackboard
        stack = inspect.stack()
        depth = 4 if 'DateBased' in self.__class__.__name__ else 3
        the_class = stack[depth][0].f_locals["self"].__class__
        the_method = stack[depth][0].f_code.co_name
        if (the_class is not date_based_blackboard.DateBasedBlackboard.__class__ or blackboard.Blackboard.__class__) and the_method is not '__init__':
            raise UserWarning('{} should not be created outside of the Blackboard class or its subclasses.'.format(self.__class__.__name__))
