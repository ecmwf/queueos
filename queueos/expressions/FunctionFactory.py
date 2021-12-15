# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

from queueos.expressions import functions

FUNCTIONS = {}


class UserFunction(functions.FunctionExpression):
    def execute(self, context, *args):
        func = self._func[0]
        return func(context, *args)


class FunctionFactory:
    """This class instantiates objects that are sub-classes of the
    FunctionExpression. Objects are created by name, by capitalising the
    first letter. So if the function name is 'foo', the class
    queueos.expressions.functions.Foo is instantiated.
    """

    @classmethod
    def create(cls, name, *args):

        # if '.' in name:
        #     assert len(args) ==0
        #     names = name.split('.')
        #     result =  cls.create(names[0])

        #     for n in names[1:]:
        #         result = cls.create('dot', result, n)
        #     return result

        if name not in FUNCTIONS:
            func = name[0].upper() + name[1:]
            func = f"Function{func}"
            func = getattr(functions, func, None)
            if func is None:
                raise ValueError(f"Cannot find a function called '{name}'")
            FUNCTIONS[name] = func
        return FUNCTIONS[name](name, args)

    @classmethod
    def register_function(cls, name, func):
        # For some reason, we cannot set _func to be a callable because
        # it becomes a method. So we wrap it in a list.
        attributes = dict(_func=[func])
        FUNCTIONS[name] = type(
            f"Function_{name}",
            (UserFunction,),
            attributes,
        )
