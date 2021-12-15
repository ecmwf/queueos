# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#


class ParserError(Exception):
    """Exception thrown by the Parser class."""

    def __init__(self, message, line):
        super().__init__(f"{message} (line {line})")


class Reader:
    def __init__(self, file):
        self.file = file
        self._c = None

    def read(self):
        if self._c is not None:
            c = self._c
            self._c = None
            return c
        return self.file.read(1)

    def peek(self):
        if self._c is None:
            self._c = self.file.read(1)
        return self._c


class Parser:
    """This class is a simple tokeniser used to parse text files or strings.
    The class returns the next non-space characters, and allow one
    character look-ahead. Comments are marked with '#' and are ignored.
    This class must be sub-classed."""

    def __init__(self, path, comments=True):
        if isinstance(path, str):
            self.reader = Reader(open(path))
        else:
            self.reader = Reader(path)
        self.comments = comments
        self.eof = False
        self.line = 0

    def read(self):

        if self.eof:
            return ""

        c = self.reader.read()
        if c == "":
            self.eof = True

        return c

    def peek(self, spaces=False):
        while True:

            c = self.reader.peek()

            if self.comments and c == "#":
                while c != "\n" and c != 0:
                    c = self.read()
                    if c == "\n":
                        self.line += 1

                if c == "":
                    return ""

                return self.peek(spaces)

            if spaces or not str.isspace(c):
                return c
            else:
                c = self.read()
                if c == "\n":
                    self.line += 1

    def next(self, spaces=False):

        while True:
            c = self.read()
            if c == "":
                raise ParserError(self, "next reached eof", self.line + 1)

            if c == "\n":
                self.line += 1

            if self.comments and c == "#":
                while c != "\n" and c != "":
                    c = self.read()
                    if c == "\n":
                        self.line += 1

                if c == "":
                    raise ParserError("next reached eof", self.line + 1)

                return self.next(spaces)

            if spaces or not str.isspace(c):
                return c

    def consume(self, s):

        for c in s:

            n = self.next()
            if c != n:
                raise ParserError(
                    f"Parser: consume expecting '{c}', got '{n}'",
                    self.line + 1,
                )
