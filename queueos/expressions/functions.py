# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import operator
import re


class FunctionExpression:
    def __init__(self, name, args):
        self.name = name
        self.args = args

    def __repr__(self):
        args = ",".join(repr(a) for a in self.args)
        return f"{self.name}({args})"

    def evaluate(self, context):
        args = [a.evaluate(context) for a in self.args]
        try:
            return self.execute(context, *args)
        except Exception as e:
            args = ",".join(repr(a) for a in args)
            print(f"{self.name}({args}): {e}")
            raise


#########################################################################################
class Constant(FunctionExpression):
    def execute(self, context):
        return self.value


class FunctionTrue(Constant):
    value = True


class FunctionFalse(Constant):
    value = False


#########################################################################################
class UnOp(FunctionExpression):
    def execute(self, context, a):
        return self.op(a)


class FunctionNeg(UnOp):
    op = operator.neg


class FunctionNot(UnOp):
    op = operator.not_


#########################################################################################
class BinOp(FunctionExpression):
    def execute(self, context, a, b):
        return self.op(a, b)


class FunctionAdd(BinOp):
    op = operator.add


class FunctionSub(BinOp):
    op = operator.sub


class FunctionDiv(BinOp):
    op = operator.truediv


class FunctionPow(BinOp):
    op = operator.pow


class FunctionMul(BinOp):
    op = operator.mul


class FunctionEq(BinOp):
    op = operator.eq


class FunctionNe(BinOp):
    op = operator.ne


class FunctionGe(BinOp):
    op = operator.ge


class FunctionGt(BinOp):
    op = operator.gt


class FunctionLe(BinOp):
    op = operator.le


class FunctionLt(BinOp):
    op = operator.lt


class FunctionAnd(BinOp):
    def op(self, a, b):
        return a and b


class FunctionOr(BinOp):
    def op(self, a, b):
        return a or b


class FunctionMatch(BinOp):
    def op(self, a, b):
        return True if re.match(b, a) else False


class FunctionDot(BinOp):
    def op(self, a, b):
        return getattr(a, b)


#########################################################################################


class Convertor(FunctionExpression):
    def execute(self, context, x):
        return self.scale * x


class FunctionSecond(Convertor):
    scale = 1


class FunctionMinute(Convertor):
    scale = 60


class FunctionHour(Convertor):
    scale = 60 * 60


class FunctionDay(Convertor):
    scale = 24 * 60 * 60


class FunctionKb(Convertor):
    scale = 1024


class FunctionMb(Convertor):
    scale = 1024 * 1024


class FunctionGb(Convertor):
    scale = 1024 * 1024 * 1024


class FunctionTb(Convertor):
    scale = 1024 * 1024 * 1024 * 1024


#########################################################################################
class FunctionIf(FunctionExpression):
    def execute(self, context, condition, true, false):
        return true if condition else false


class FunctionInfinity(FunctionExpression):
    def execute(self, context):
        return float("inf")


class FunctionNumberOfWorkers(FunctionExpression):
    def execute(self, context):
        return context.request.dispatcher.number_of_workers


class FunctionUser(FunctionExpression):
    def execute(self, context):
        return context.request.user


# class FunctionAdaptor(FunctionExpression):
#     def execute(self, context):
#         return request.adaptor


class FunctionAvailable(FunctionExpression):
    def execute(self, context, resource):
        return context.environment.resource_enabled(resource)


# class FunctionDataset(FunctionExpression):
#     def execute(self, context):
#         return request.dataset


class FunctionEstimatedSize(FunctionExpression):
    def execute(self, context):
        return context.request.cost[0]


class FunctionEstimatedTime(FunctionExpression):
    def execute(self, context):
        return context.request.cost[1]


class FunctionRequest(FunctionExpression):
    def execute(self, context):
        return context.request
