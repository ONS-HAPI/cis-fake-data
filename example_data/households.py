"""
Generate fake data for households survey raw input data.
"""
from mimesis.schema import Field, Schema
import pandas as pd

_ = Field('en-gb', seed=42)