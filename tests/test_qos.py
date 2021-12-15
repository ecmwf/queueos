import io

from queueos import Environment, FunctionFactory
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

# request = Request(
#     {},
#     dict(
#         user="david",
#         dataset="dataset-1",
#         adaptor="adaptor1",
#         cost=(1024 * 1024, 60 * 60 * 24),
#     ),
# )


environment = Environment()
environment.disable_resource("adaptor2")


def compile(text):
    parser = RulesParser(io.StringIO(text))
    rules = RuleSet()
    parser.parse_rules(rules, environment)
    return rules


# def test_rules():
#     rules = compile(
#         """
#     user "Limit for david"       (user == "david") : 5
#                     """
#     )
#     assert len(rules.user_limits) == 1
#     assert rules.user_limits[0].match(request)
