import inspect

class BaseManager():

    def __init__(self, parent):
        self.checkCaller()
        self._parent = parent

    def checkCaller(self):
        from macsy.blackboards import blackboard, date_based_blackboard
        stack = inspect.stack()
        the_class = stack[2][0].f_locals["self"].__class__
        the_method = stack[2][0].f_code.co_name
        if (the_class is not date_based_blackboard.DateBasedBlackboard.__class__ or blackboard.Blackboard.__class__) and the_method is not '__init__':
            raise PermissionError('{} should not be created outside of the Blackboard class or its subclasses.'.format(self.__class__.__name__))