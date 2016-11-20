""" Tests package initialization. """

__author__ = "Vince Reuter"
__modified__ = "2016-11-19"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.__init__"


class abstractclassmethod(classmethod):

    __isabstracmethod__ = True

    def __init__(self, func):
        """
        Set the abstract method property for given function
        and leverage ordinary classmethod constructor.

        :param callable func: function to designate as an abstract class method
        """
        func.__isabstractmethod__ = True
        super(abstractclassmethod, self).__init__(func)
