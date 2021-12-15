# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

from queueos.expressions.FunctionFactory import FunctionFactory
from queueos.expressions.NumberExpression import NumberExpression
from queueos.expressions.StringExpression import StringExpression

from .Parser import Parser, ParserError

OPERATORS = {
    "+": "add",
    "-": "sub",
    "*": "mul",
    "/": "div",
    "^": "pow",
    ">": "gt",
    ">=": "ge",
    "<": "lt",
    "<=": "le",
    "==": "eq",
    "!=": "ne",
    "&&": "and",
    "||": "or",
    "~": "match",
}


class RulesParser(Parser):
    """This class implements a simple recursive descent parser to crack a
    text file containing rules (broker.rules file) and fills the
    RuleSet that is provided as argument."""

    def parse_ident(self):
        s = ""
        c = self.peek(True)
        while str.isidentifier(c) or c == ".":
            s += self.next()
            c = self.peek(True)

        # print(f'ident [{s}]')
        return s

    def parse_number(self):
        s = ""

        while str.isdigit(self.peek(True)):
            s += self.next()

        if self.peek(True) == ".":
            s += self.next()
            c = self.next()
            if not str.isdigit(self, c):
                raise ParserError(
                    "parseNumber invalid '{c}'",
                    self.line + 1,
                )

            s += c
            while str.isdigit(self, self.peek(True)):
                s += self.next()

        c = self.peek(True)
        if c == "e" or c == "E":
            s += self.next()

            c = self.next()
            if c == "-" or c == "+":
                s += c
                c = self.next()

            if not str.isdigit(self, c):
                raise ParserError(
                    f"parseNumber invalid '{c}'",
                    self.line + 1,
                )

            s += c
            while str.isdigit(self, self.peek()):
                s += self.next()

        try:
            return NumberExpression(int(s))
        except ValueError:
            return NumberExpression(float(s))

    def parse_string(self):
        quote = self.peek()

        if quote not in ("'", '"'):
            raise ParserError(
                f"Invalid quote: {quote}",
                self.line + 1,
            )

        self.consume(quote)
        s = ""
        while True:
            c = self.next(True)
            if c == quote:
                break

            s += c

        return StringExpression(s, quote)

    def parse_atom(self):

        c = self.peek()
        if c == "(":
            self.consume("(")
            e = self.parse_disjunction()
            self.consume(")")
            return e

        if c == "-":
            self.consume("-")
            return FunctionFactory.create("neg", self.parse_atom())

        if c == "!":
            self.consume("!")
            return FunctionFactory.create("not", self.parse_atom())

        if c == "'":
            return self.parse_string()

        if c == '"':
            return self.parse_string()

        while str.isalpha(c) or c == "_":
            name = self.parse_ident()
            if self.peek() == "(":
                args = self.parse_list()
                return FunctionFactory.create(name, *args)
            else:
                return FunctionFactory.create(name)

        if str.isdigit(c):
            return self.parse_number()
        else:
            raise ParserError(
                f"parse_atom: invalid '{c}'",
                self.line + 1,
            )

    def parse_power(self):

        result = self.parse_atom()
        c = self.peek()
        while c == "^":
            self.consume(c)
            result = FunctionFactory.create(
                "pow",
                result,
                self.parse_atom(),
            )
            c = self.peek()

        return result

    def parse_list(self):
        result = []
        self.consume("(")
        while self.peek() != ")":
            result.append(self.parse_expression())
            if self.peek() == ")":
                break

            self.consume(",")

        self.consume(")")
        return result

    def parse_factor(self):

        result = self.parse_power()
        c = self.peek()
        while c in ("*", "/"):
            self.consume(c)
            result = FunctionFactory.create(
                OPERATORS[c],
                result,
                self.parse_power(),
            )
            c = self.peek()

        return result

    def parse_term(self):

        result = self.parse_factor()
        c = self.peek()
        while c in ("+", "-"):
            self.consume(c)
            result = FunctionFactory.create(
                OPERATORS[c],
                result,
                self.parse_factor(),
            )
            c = self.peek()

        return result

    def parse_test(self):

        result = self.parse_term()
        c = self.peek()
        while c in ("<", ">", "=", "!", "~"):

            self.consume(c)

            name = "" + c

            c = self.peek()
            if c == "=":
                self.consume(c)
                name += c

            result = FunctionFactory.create(
                OPERATORS[name],
                result,
                self.parse_term(),
            )
            c = self.peek()

        return result

    def parse_conjunction(self):

        result = self.parse_test()
        c = self.peek()
        while c == "&":

            self.consume(c)
            self.consume(c)

            result = FunctionFactory.create(
                "and",
                result,
                self.parse_test(),
            )
            c = self.peek()

        return result

    def parse_disjunction(self):

        result = self.parse_conjunction()
        c = self.peek()
        while c == "|":

            self.consume(c)
            self.consume(c)

            result = FunctionFactory.create(
                "or",
                result,
                self.parse_conjunction(),
            )
            c = self.peek()

        return result

    def parse_permission(self, rules, environment):
        info = self.parse_string()
        condition = self.parse_expression()
        self.consume(":")
        conclusion = self.parse_expression()

        rules.add_permission(environment, info, condition, conclusion)

    def parse_priority(self, rules, environment):
        info = self.parse_string()
        condition = self.parse_expression()
        self.consume(":")
        conclusion = self.parse_expression()

        rules.add_priority(environment, info, condition, conclusion)

    def parse_global_limit(self, rules, environment):

        info = self.parse_string()
        condition = self.parse_expression()
        self.consume(":")
        conclusion = self.parse_expression()

        rules.add_global_limit(environment, info, condition, conclusion)

    def parse_user_limit(self, rules, environment):

        info = self.parse_string()
        condition = self.parse_expression()
        self.consume(":")
        conclusion = self.parse_expression()

        rules.add_user_limit(environment, info, condition, conclusion)

    def parse_expression(self):
        return self.parse_disjunction()

    def parse(self):
        """Parse the text provided in the constructor, which should represent a
        single expression. This method is used for unit testing."""
        result = self.parse_expression()
        c = self.peek()
        if c != "":
            raise ParserError(
                f"Error parsing rules: remaining char: {c} ({ord(c)})",
                self.line + 1,
            )

        return result

    def parse_rules(self, rules, environment):
        """Parse the text provided in the constructor

        Args:
            rules ([type]): the rules that will be updated with the rules
            environment ([type]): the environment in which the rules will be evaluated

        Raises:
            ParserError: [description]
        """

        while self.peek():

            ident = self.parse_ident()

            if ident == "limit":
                self.parse_global_limit(rules, environment)
                continue

            if ident == "priority":
                self.parse_priority(rules, environment)
                continue

            if ident == "permission":
                self.parse_permission(rules, environment)
                continue

            if ident == "user":
                self.parse_user_limit(rules, environment)
                continue

            raise ParserError(f"Unknown rule: '{ident}'", self.line + 1)
