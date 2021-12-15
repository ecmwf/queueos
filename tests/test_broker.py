import io
import time

from queueos import Broker, Environment, FunctionFactory, Request, Status
from queueos.expressions.RulesParser import RulesParser
from queueos.qos.Rule import RuleSet

FunctionFactory.register_function(
    "dataset",
    lambda context, *args: context.request.dataset,
)
FunctionFactory.register_function(
    "adaptor",
    lambda context, *args: context.request.adaptor,
)


class SimpleRequest(Request):

    dataset = "dataset-1"
    adaptor = "adaptor1"
    cost = (1024 * 1024, 60 * 60 * 24)

    def __init__(self, user):
        super().__init__()
        self.user = user

    def execute(self):
        # time.sleep(0.1)
        self.time = time.time()


environment = Environment()
environment.disable_resource("adaptor2")


def compile(text):
    parser = RulesParser(io.StringIO(text))
    rules = RuleSet()
    parser.parse_rules(rules, environment)
    return rules


RULES1 = compile(
    """
priority "david"    (user == "david") : 100
priority "frank"    (user == "frank") : 10
priority "erin"     (user == "erin") : 1
"""
)


def test_priorities():
    broker = Broker(RULES1, 1, environment)
    broker.pause()
    a = SimpleRequest("erin")
    broker.enqueue(a)
    c = SimpleRequest("frank")
    broker.enqueue(c)
    b = SimpleRequest("david")
    broker.enqueue(b)
    broker.resume()
    broker.shutdown()

    assert a.status == Status.COMPLETE
    assert b.status == Status.COMPLETE
    assert c.status == Status.COMPLETE

    assert a.time > b.time
    assert c.time > b.time
    assert a.time > c.time


RULES2 = compile(
    """
priority "david"    (user == "david") : 100
priority "frank"    (user == "frank") : 10
priority "erin"     (user == "erin") : 1
"""
)


def test_global_limits():
    broker = Broker(RULES2, 1, environment)
    broker.pause()
    a = SimpleRequest("erin")
    broker.enqueue(a)
    c = SimpleRequest("frank")
    broker.enqueue(c)
    b = SimpleRequest("david")
    broker.enqueue(b)
    broker.resume()
    broker.shutdown()

    assert a.status == Status.COMPLETE
    assert b.status == Status.COMPLETE
    assert c.status == Status.COMPLETE

    assert a.time > b.time
    assert c.time > b.time
    assert a.time > c.time


if __name__ == "__main__":
    test_priorities()
