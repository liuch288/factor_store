"""FactorStore 工具函数：路径构建、参数校验、类型转换、对齐检查。"""

from __future__ import annotations

import datetime
import os
import re
from pathlib import Path
from typing import Union

import polars as pl

VALID_FREQUENCIES: set[str] = {"tick", "1min", "5min"}


class AlignmentError(Exception):
    """时间戳对齐校验失败异常。"""
    pass


def resolve_root_path(root_path: str | None) -> Path:
    """解析根路径：优先传入值 → 环境变量 FACTORSTORE_ROOT → ~/data/factor_store。"""
    if root_path is not None:
        p = Path(root_path)
    else:
        env = os.environ.get("FACTORSTORE_ROOT")
        if env:
            p = Path(env)
        else:
            p = Path.home() / "data" / "factor_store"
    if not p.is_absolute():
        raise ValueError(f"root_path 必须是绝对路径: {p}")
    return p


def normalize_trade_date(trade_date: Union[str, datetime.date]) -> str:
    """将 trade_date 规范为 YYYY-MM-DD 格式。支持 datetime.date、YYYY-MM-DD、YYYYMMDD。"""
    if isinstance(trade_date, datetime.datetime):
        return trade_date.strftime("%Y-%m-%d")
    if isinstance(trade_date, datetime.date):
        return trade_date.isoformat()
    if isinstance(trade_date, str):
        cleaned = trade_date.replace("-", "")
        if not re.fullmatch(r"\d{8}", cleaned):
            raise ValueError(f"trade_date 格式不合法，期望 YYYY-MM-DD 或 YYYYMMDD，收到: {trade_date}")
        return f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:8]}"
    raise TypeError(f"trade_date 必须是 str 或 datetime.date，收到: {type(trade_date)}")


def validate_frequency(frequency: str) -> None:
    """校验频率参数。"""
    if frequency not in VALID_FREQUENCIES:
        raise ValueError(f"频率必须是 tick、1min 或 5min，收到: {frequency}")


def validate_dataframe(df: pl.DataFrame) -> None:
    """校验 DataFrame 必须包含 ts 列。"""
    if "ts" not in df.columns:
        raise ValueError("DataFrame 必须包含 ts 列")


def build_partition_path(
    root_path: Path, frequency: str, contract: str, trade_date: str,
) -> Path:
    """构建 Hive 风格分区目录路径。"""
    return root_path / frequency / contract / trade_date


def build_factor_path(
    root_path: Path, frequency: str, contract: str, trade_date: str, factor_name: str,
) -> Path:
    """构建因子文件完整路径。"""
    return build_partition_path(root_path, frequency, contract, trade_date) / f"{factor_name}.parquet"


def convert_ts_column(df: pl.DataFrame) -> pl.DataFrame:
    """将 ts 列转换为 timestamp[ns] 类型。"""
    ts_dtype = df["ts"].dtype
    if ts_dtype != pl.Datetime("ns"):
        df = df.with_columns(pl.col("ts").cast(pl.Datetime("ns")))
    return df


def cast_to_float64(df: pl.DataFrame, factor_name: str) -> pl.DataFrame:
    """将除 ts 列外的所有列转换为 float64。"""
    casts = []
    for col_name in df.columns:
        if col_name == "ts":
            continue
        dtype = df[col_name].dtype
        if dtype == pl.Float64:
            continue
        if dtype in (pl.Utf8, pl.String, pl.Boolean, pl.Date, pl.Time, pl.Categorical):
            raise TypeError(f"列 '{col_name}' 无法转换为 float64 类型")
        casts.append(pl.col(col_name).cast(pl.Float64))
    if casts:
        df = df.with_columns(casts)
    return df


def add_column_prefix(df: pl.DataFrame, factor_name: str) -> pl.DataFrame:
    """为除 ts 列外的所有列添加 {factor_name}_ 前缀。"""
    renames = {
        col: f"{factor_name}__{col}" for col in df.columns if col != "ts"
    }
    return df.rename(renames)


def check_alignment(
    partition_path: Path, new_df: pl.DataFrame, exclude_factor: str,
) -> None:
    """检查新 DataFrame 与分区内已有因子的对齐性。"""
    if not partition_path.exists():
        return
    for f in partition_path.iterdir():
        if not f.suffix == ".parquet":
            continue
        if f.stem == exclude_factor:
            continue
        existing = pl.read_parquet(f, columns=["ts"])
        new_ts = new_df.select("ts")
        if existing.height != new_ts.height:
            raise AlignmentError(
                f"对齐校验失败: 已有因子行数为 {existing.height}，新因子行数为 {new_ts.height}"
            )
        diff_mask = existing["ts"] != new_ts["ts"]
        if diff_mask.any():
            idx = diff_mask.arg_true()[0]
            raise AlignmentError(
                f"对齐校验失败: 时间戳列与已有因子不一致（首个不同位置: 第 {idx} 行）"
            )
        break  # 只需与一个已有因子对比即可


def cleanup_empty_dirs(path: Path, root_path: Path) -> None:
    """从 path 向上递归删除空目录，直到 root_path。"""
    current = path
    while current != root_path and current.is_relative_to(root_path):
        if current.exists() and current.is_dir() and not any(current.iterdir()):
            current.rmdir()
        else:
            break
        current = current.parent
