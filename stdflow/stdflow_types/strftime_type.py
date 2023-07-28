from __future__ import annotations

import re

try:
    from typing import Literal, Protocol, Union
except ImportError:
    from typing_extensions import Literal, Protocol, Union


def is_strftime(arg: str) -> bool:
    strftime_regex = re.compile(r"%[YmdHIMSjUWcXxGgpVzZ%]")
    return strftime_regex.search(arg) is not None


class StringProtocol(Protocol):
    @staticmethod
    def __call__(arg: str) -> bool:
        ...


class Strftime(StringProtocol):
    @staticmethod
    def __call__(arg: str) -> bool:
        return isinstance(arg, str) and is_strftime(arg)


if __name__ == "__main__":
    ValidArgument = Union[Literal[":default", ":auto"], Strftime]

    #
    #
    def check_argument(arg: ValidArgument):
        return isinstance(arg, str)

    # ValidArgument.is

    # usage
    # print(check_argument(':default') is True)
    # print(check_argument(':auto') == True)
    # print(check_argument('invalid'))
    # print(check_argument(3) == False)
    # print(Strftime.__call__('coucou %Y %m %d') == True)
    #
    print(Strftime.__call__("coucou %Y %m %d"))
    print(Strftime.__call__("invalid"))
    print(Strftime.__call__(3))
