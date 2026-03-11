"""共享 fixtures 和 hypothesis 策略。"""

from __future__ import annotations

import datetime

import polars as pl
import pytest
from hypothesis import strategies as st

from factorstore import FactorStore

# ---------- pytest fixtures ----------

@pytest.fixture
def store(tmp_path):
    """创建一个使用临时目录的 FactorStore 实例。"""
    return FactorStore(root_path=str(tmp_path / "factor_data"))


# ---------- hypothesis 策略 ----------

frequencies = st.sampled_from(["tick", "1min", "5min"])

contracts = st.from_regex(r"[A-Z]{2}\d{4}", fullmatch=True)

trade_dates = st.dates(
    min_value=datetime.date(2020, 1, 1),
    max_value=datetime.date(2030, 12, 31),
).map(lambda d: d.strftime("%Y%m%d"))

factor_names = st.from_regex(r"[a-z][a-z0-9]{2,10}", fullmatch=True)

# 生成合法的列名（不含 ts）
_col_names = st.from_regex(r"[a-z][a-z0-9]{1,8}", fullmatch=True)


@st.composite
def factor_dataframes(draw, n_rows=None, n_cols=None):
    """生成包含 ts 列和随机 float64 因子值列的 Polars DataFrame。"""
    if n_rows is None:
        n_rows = draw(st.integers(min_value=1, max_value=50))
    if n_cols is None:
        n_cols = draw(st.integers(min_value=1, max_value=4))

    # 生成唯一列名
    cols = draw(
        st.lists(_col_names, min_size=n_cols, max_size=n_cols, unique=True)
    )

    # 生成时间戳（排序、唯一）
    base = datetime.datetime(2025, 1, 1, 9, 30, 0)
    ts_values = [base + datetime.timedelta(seconds=i) for i in range(n_rows)]

    data: dict[str, list] = {"ts": ts_values}
    for col in cols:
        data[col] = draw(
            st.lists(
                st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
                min_size=n_rows,
                max_size=n_rows,
            )
        )

    return pl.DataFrame(data)
