import re


# utility functions for using mocks in unit tests
class TestMockHelpers:
    # applies a context manager's context to an entire test, automatically
    # cleaning it up after the test completes. useful for adding (e.g.) a
    # patch to an entire test without needing to wrap it in a 'with' statement.
    def apply_context_manager(self, context_manager):
        val = context_manager.__enter__()
        self.addCleanup(context_manager.__exit__, None, None, None)
        return val
