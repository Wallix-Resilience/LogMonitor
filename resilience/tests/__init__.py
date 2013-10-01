import sys


def egg_test_runner():
    """
    Test collector and runner for setup.py test
    """
    from twisted.scripts.trial import run
    original_args = list(sys.argv)
    sys.argv = ["", "resilience"]
    try:
        return run()
    finally:
        sys.argv = original_args
