# %%
import pandas as pd
from typing import List
from tqdm import tqdm

from gremlin_python.process.graph_traversal import GraphTraversalSource, __
from gremlin_python.process.traversal import (
    Barrier,
    Bindings,
    Cardinality,
    Column,
    Direction,
    Operator,
    Order,
    P,
    Pop,
    Scope,
    T,
    WithOptions,
)
