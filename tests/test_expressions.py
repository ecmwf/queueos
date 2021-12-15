import io

from queueos import Environment, FunctionFactory
from queueos.expressions.RulesParser import RulesParser
from queueos.qos.Rule import Context

FunctionFactory.register_function(
    "dataset",
    lambda context, *args: context.request.dataset,
)
FunctionFactory.register_function(
    "adaptor",
    lambda context, *args: context.request.adaptor,
)


class TestRequest:

    user = "david"
    dataset = "dataset-1"
    adaptor = "adaptor1"
    cost = (1024 * 1024, 60 * 60 * 24)


request = TestRequest()


environment = Environment()
environment.disable_resource("adaptor2")


def compile(text):
    parser = RulesParser(io.StringIO(text))
    return parser.parse()


def evaluate(text):
    return compile(text).evaluate(Context(request, environment))


def test_expression():

    assert evaluate("1 + 2") == 3.0
    assert evaluate("1 - 2") == -1.0
    assert evaluate("1 / 2") == 0.5
    assert evaluate("2 * 3") == 6.0
    assert evaluate("2 ^ 10") == 1024.0
    assert evaluate("2 + 3 * 5") == 17.0
    assert evaluate("(2+3) * 5") == 25.0
    assert evaluate("(2 + 3) * -5") == -25.0

    assert evaluate("1 > 2") is False
    assert evaluate("3 > 3") is False
    assert evaluate("3 > 2") is True

    assert evaluate("3 >= 2") is True
    assert evaluate("3 >= 3") is True
    assert evaluate("2 >= 3") is False

    assert evaluate("1 < 2") is True
    assert evaluate("3 < 3") is False
    assert evaluate("3 < 2") is False

    assert evaluate("3 <= 2") is False
    assert evaluate("3 <= 3") is True
    assert evaluate("2 <= 3") is True

    assert evaluate("2<=3 || 1>2") is True
    assert evaluate("2>=3 || 1>2") is False
    assert evaluate("3>=3 || 5>2") is True

    assert evaluate("2<=3 && 2>1") is True
    assert evaluate("2>=3 && 1>2") is False

    assert evaluate("5 - 1 != 1 - 5") is True
    assert evaluate("2 + 4 == 8 - 2") is True

    assert evaluate("2 + 4 == 8") is False
    assert evaluate("!(2 + 4 == 8)") is True

    assert evaluate("'abcd' ~ '^.*d$'") is True

    assert evaluate("true") is True
    assert evaluate("false") is False

    assert evaluate(" 'a' + 'b' ") == "ab"


def test_functions():

    assert evaluate("second(1)") == 1
    assert evaluate("minute(1)") == 60
    assert evaluate("hour(1)") == 3600
    assert evaluate("day(1)") == 86400

    assert evaluate("Kb(1)") == 1024
    assert evaluate("Mb(1)") == 1024 * 1024
    assert evaluate("Gb(1)") == 1024 * 1024 * 1024
    assert evaluate("Tb(1)") == 1024 * 1024 * 1024 * 1024

    assert evaluate("if(1 > 2, 42, 69)") == 69
    assert evaluate("if(1 < 2, 42, 69)") == 42


def test_requests():
    assert evaluate("user") == "david"
    assert evaluate("adaptor") == "adaptor1"
    assert evaluate("infinity") == float("inf")
    assert evaluate("available(adaptor)") is True
    assert evaluate("available('adaptor2')") is False
    assert evaluate("estimatedSize") == 1024 * 1024
    assert evaluate("estimatedTime") == 24 * 60 * 60


# def test_bits():
#     assert evaluate("request.user") == "david"
