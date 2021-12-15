# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#


class Context:
    def __init__(self, request, environment):
        self.request = request
        self.environment = environment


class QoSRule:
    """
    This class represents an  QoS rule. Rules have two parts: the
    'condition' and the 'conclusion', which are both Expressions. The
    'condition' is used to match request, while the 'conclusion' is used
    to perform an action with the matching request. For example, in the
    case of a 'Limit', the conclusion represents the maximum value
    (self,capacity) of that limit. For 'Permission', the conclusion must
    evaluate to a  that states if the request is accepted or
    denied. For 'Priority', the conclusion must evaluate to a starting
    priority for the request.

    The 'info' is currently simply a string that explains the rule. It is
    an expression, so it can be evaluated dynamically later.
    """

    def __init__(self, environment, info, condition, conclusion):
        self.environment = environment
        self.info = info
        self.condition = condition
        self.conclusion = conclusion

    def evaluate(self, request):
        return self.conclusion.evaluate(Context(request, self.environment))

    def match(self, request):
        return self.condition.evaluate(Context(request, self.environment))

    def dump(self, out):
        out(self)

    def __repr__(self):
        return f"{self.name} {self.info} {self.condition} : {self.conclusion}"


class Priority(QoSRule):
    """
    This class represents a priority rule. The 'conclusion' part must
    evaluate as a number, which is then used to compute the starting
    priority of a request. All rules matching a request contributes to
    the starting priority of the request. The priority is a number that
    represents a number of seconds. For example, a request with a
    starting priority of 120 will overtake all requests of priority zero
    added to the queue in the last 2 minutes. Priorities are only used to
    decide which request to run next. It does not affect already running
    requests.

    The request priority is equal to its starting priority plus its time
    in the queue. So if a request as a starting priority of zero and
    stays in the queue for 1 hour, its priority will be 3600. Another way
    of understanding this algorithm is to say that the priority of each
    queued request is increased by one every second. This will ensure
    that requests starting with a very low priority will eventually be
    scheduled, and will not always be overtaken by higher priority
    requests.
    """

    name = "priority"


class Permission(QoSRule):
    """
    This class implements the permission rule. Its 'conclusion' must
    evaluate to a . If the evaluation returns True, the matching
    requests are granted execution, otherwise they are denied execution
    and are immediately set to aborted.
    """

    name = "permission"


class Limit(QoSRule):
    """
    This class implements a limit in the QoS system. Its 'conclusion'
    must evaluate to a positive integer, which represent the 'capacity'
    of the limit, i.e. the number of requests that this rule will allow
    to execute simultaneously.

    A limit holds a counter that is incremented each time a request
    matching the 'condition' part of the rule is started, and decremented
    when the request finishes. If the counter value reaches the maximum
    capacity of the limit, no requests matching that limit can run.
    """

    def __init__(self, environment, info, condition, conclusion):
        super().__init__(environment, info, condition, conclusion)
        self.value = 0

    def increment(self):
        self.value += 1

    def decrement(self):
        if self.value > 0:
            self.value -= 1

    def capacity(self, request):
        return self.evaluate(request)

    def full(self, request):
        # NOTE: the self.value can be greater than the limit capacity after a
        # reconfiguration of the QoS
        return self.value >= self.capacity(request)


class GlobalLimit(Limit):
    """
    This is a subclass of 'Limit' and exists for the sake of strong
    typing. Global limits are shared by all users.
    """

    name = "limit"


class UserLimit(Limit):

    name = "user"

    def clone(self):
        return UserLimit(
            self.environment,
            self.info,
            self.condition,
            self.conclusion,
        )


class RuleSet:
    """
    This utility class is used to store all rules in a single neat an
    tidy location.
    """

    def __init__(self):
        self.priorities = []
        self.global_limits = []
        self.permissions = []
        self.user_limits = []

    def add_priority(self, environment, info, condition, conclusion):
        self.priorities.append(Priority(environment, info, condition, conclusion))

    def add_permission(self, environment, info, condition, conclusion):
        self.permissions.append(Permission(environment, info, condition, conclusion))

    def add_user_limit(self, environment, info, condition, conclusion):
        self.user_limits.append(UserLimit(environment, info, condition, conclusion))

    def add_global_limit(self, environment, info, condition, conclusion):
        self.global_limits.append(GlobalLimit(environment, info, condition, conclusion))

    def dump(self, out=print):
        out()
        out("# Permissions:")
        out()
        for p in self.permissions:
            p.dump(out)

        out()
        out("# Global limits:")
        out()
        for p in self.global_limits:
            p.dump(out)

        out()
        out("# Per user limits:")
        out(self)
        for p in self.user_limits:
            p.dump(out)

        out()
        out("# Priorities:")
        out()
        for p in self.priorities:
            p.dump(out)
