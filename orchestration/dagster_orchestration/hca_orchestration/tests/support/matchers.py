# Classes with more permissive __eq__ functions for use as argument matchers for mocks.

# Matches if a mock was called with a string containing the given substring
class StringContaining(str):
    def __eq__(self, other_str):
        return isinstance(other_str, str) and self in other_str

    def __repr__(self):
        return f"(string containing '{super().__str__()}')"
