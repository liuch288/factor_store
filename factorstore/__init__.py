"""FactorStore —— 轻量级多频率因子 Parquet 存储与查询引擎。"""

from .core import FactorStore
from .dominant import DominantAlias, parse_alias, resolve_contract
from .utils import AlignmentError

__all__ = [
    "FactorStore",
    "AlignmentError",
    "DominantAlias",
    "parse_alias",
    "resolve_contract",
]
