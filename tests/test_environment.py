"""Smoke tests to verify all required libraries are importable."""


def test_climaccf():
    import climaccf  # noqa: F401


def test_pycontrails():
    import pycontrails  # noqa: F401


def test_openap():
    import openap  # noqa: F401


def test_bluesky():
    import bluesky  # noqa: F401


def test_sklearn():
    import sklearn  # noqa: F401


def test_torch():
    import torch  # noqa: F401


def test_xarray():
    import xarray  # noqa: F401


def test_pandas():
    import pandas  # noqa: F401


def test_numpy():
    import numpy  # noqa: F401
