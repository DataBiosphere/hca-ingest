# Classes with more permissive __eq__ functions for use as argument matchers for mocks.

import re


# Matches if a mock was called with a string containing the given substring
class StringContaining(str):
    def __eq__(self, other_str: object) -> bool:
        return isinstance(other_str, str) and self in other_str

    def __repr__(self) -> str:
        return f"(string containing '{super().__str__()}')"


class StringStartingWith(str):
    def __eq__(self, other_str: object) -> bool:
        return isinstance(other_str, str) and other_str.startswith(self)

    def __repr__(self) -> str:
        return f"(string starting with '{super().__str__()}')"


class StringEndingWith(str):
    def __eq__(self, other_str: object) -> bool:
        return isinstance(other_str, str) and other_str.endswith(self)

    def __repr__(self) -> str:
        return f"(string ending with '{super().__str__()}')"


# doesn't currently accept compiled regexes
class StringMatchingRegex(str):
    def __eq__(self, other_str: object) -> bool:
        return isinstance(other_str, str) and re.match(self, other_str) is not None

    def __repr__(self) -> str:
        return f"(string matching regex '{super().__str__()}')"

    def __hash__(self):
        return super().__hash__()


# matches any instance of the specified type (or a descendant)
class ObjectOfType:
    def __init__(self, expected_type: type):
        self.expected_type = expected_type

    def __eq__(self, other_obj: object) -> bool:
        return isinstance(other_obj, self.expected_type)

    def __repr__(self) -> str:
        return f"(any {self.expected_type.__name__})"


# lets us do partial matching on objects without needing to make assertions about every attribute,
# e.g. "any Book with title='abc'": ObjectWithAttributes(Book, title='abc')
class ObjectWithAttributes(ObjectOfType):
    def __init__(self, expected_type: type, **expected_attributes):
        super().__init__(expected_type)
        self.attributes_dict = expected_attributes

    def __eq__(self, other_obj: object) -> bool:
        return super().__eq__(other_obj) and all(
            hasattr(other_obj, attr_name) and getattr(other_obj, attr_name) == attr_value
            for attr_name, attr_value in self.attributes_dict.items()
        )

    def __repr__(self) -> str:
        attr_assertions = [
            f"{attr_name}={repr(attr_value)}"
            for attr_name, attr_value in self.attributes_dict.items()
        ]
        return f"(any {self.expected_type.__name__} with {', '.join(attr_assertions)})"
