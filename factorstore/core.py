"""FactorStore 主类：因子数据管理引擎。"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Optional, Union

import polars as pl
import pandas as pd

from .utils import (
    AlignmentError,
    add_column_prefix,
    build_factor_path,
    build_partition_path,

    check_alignment,
    cleanup_empty_dirs,
    convert_ts_column,
    normalize_trade_date,
    resolve_root_path,
    validate_dataframe,
    validate_frequency,
)


class FactorStore:
    """因子数据管理引擎主类。"""

    def __init__(self, root_path: Optional[str] = None, use_pandas: bool = True) -> None:
        self._root_path = resolve_root_path(root_path)
        self._root_path.mkdir(parents=True, exist_ok=True)
        self._use_pandas = use_pandas

    @property
    def root_path(self) -> Path:
        return self._root_path

    def save_factor(
        self,
        contract: str,
        trade_date: Union[str, datetime.date],
        factor_name: str,
        df,
        frequency: str = "tick",
        add_prefix: bool = False,
    ) -> None:
        trade_date = normalize_trade_date(trade_date)
        # 自动将 Pandas DataFrame 转为 Polars
        if not isinstance(df, pl.DataFrame):
            try:
                if isinstance(df, pd.DataFrame):
                    # 将时间 index 转为 ts 列
                    df = df.copy()
                    df.index.name = "ts"
                    df = df.reset_index()
                df = pl.from_pandas(df)
            except Exception:
                raise TypeError("df 必须是 Polars DataFrame 或 Pandas DataFrame")
        validate_frequency(frequency)
        validate_dataframe(df)
        df = convert_ts_column(df)
        if add_prefix:
            df = add_column_prefix(df, factor_name)
        # 校验列名：双下划线 __ 最多出现一次，且不能有连续三个及以上下划线
        for col in df.columns:
            if col == "ts":
                continue
            if "___" in col or col.count("__") > 1:
                raise ValueError(f"列名 '{col}' 中包含非法的连续下划线，双下划线 '__' 仅允许作为分隔符出现一次")

        # 校验带前缀的列名是否来自同一因子
        prefixes = set()
        for col in df.columns:
            if col == "ts":
                continue
            if "__" in col:
                prefixes.add(col.split("__")[0])
        if len(prefixes) > 1:
            raise ValueError(f"列名前缀不一致，检测到多个因子: {sorted(prefixes)}，同一文件的子指标必须来自同一因子")

        factor_path = build_factor_path(
            self._root_path, frequency, contract, trade_date, factor_name,
        )
        partition_path = factor_path.parent
        partition_path.mkdir(parents=True, exist_ok=True)

        check_alignment(partition_path, df, exclude_factor=factor_name)
        df.write_parquet(factor_path, compression="zstd")

    def load_factors(
        self,
        contract: str,
        trade_date: Union[str, datetime.date],
        factor_names: list[str],
        frequency: str = "tick",
    ) -> pl.DataFrame:
        trade_date = normalize_trade_date(trade_date)
        validate_frequency(frequency)
        dfs: list[pl.DataFrame] = []
        for name in factor_names:
            path = build_factor_path(
                self._root_path, frequency, contract, trade_date, name,
            )
            if not path.exists():
                raise FileNotFoundError(f"因子文件不存在: {path}")
            dfs.append(pl.read_parquet(path))

        if len(dfs) == 1:
            result = dfs[0]
        else:
            base_ts = dfs[0]["ts"]
            result = dfs[0]
            for i, other in enumerate(dfs[1:], 1):
                if other.height != base_ts.len() or (other["ts"] != base_ts).any():
                    raise AlignmentError(
                        f"读取对齐校验失败: 因子 '{factor_names[i]}' 的 ts 列与 '{factor_names[0]}' 不一致"
                    )
                other_cols = [c for c in other.columns if c != "ts"]
                result = result.hstack(other.select(other_cols))

        if self._use_pandas:
            result_pd = result.to_pandas()
            result_pd = result_pd.set_index("ts")
            return result_pd
        return result

    def list_factors(
        self, contract: str, trade_date: Union[str, datetime.date], frequency: str = "tick",
    ) -> list[str]:
        trade_date = normalize_trade_date(trade_date)
        validate_frequency(frequency)
        partition = build_partition_path(
            self._root_path, frequency, contract, trade_date,
        )
        if not partition.exists():
            return []
        return sorted(
            f.stem for f in partition.iterdir() if f.suffix == ".parquet"
        )

    def exists(
        self, contract: str, trade_date: Union[str, datetime.date], factor_name: str, frequency: str = "tick",
    ) -> bool:
        trade_date = normalize_trade_date(trade_date)
        return build_factor_path(
            self._root_path, frequency, contract, trade_date, factor_name,
        ).exists()

    def delete_factor(
        self, contract: str, trade_date: Union[str, datetime.date], factor_name: str, frequency: str = "tick",
    ) -> None:
        trade_date = normalize_trade_date(trade_date)
        path = build_factor_path(
            self._root_path, frequency, contract, trade_date, factor_name,
        )
        if not path.exists():
            raise FileNotFoundError(f"因子文件不存在: {path}")
        path.unlink()
        cleanup_empty_dirs(path.parent, self._root_path)
