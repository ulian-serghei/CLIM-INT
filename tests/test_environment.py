"""Smoke tests to verify all required libraries are importable."""

def test_climaccf():
    import climaccf

def test_pycontrails():
    import pycontrails

def test_openap():
    import openap

def test_bluesky():
    import bluesky

def test_core_libraries():
    import sklearn, torch, xarray, pandas, numpy
