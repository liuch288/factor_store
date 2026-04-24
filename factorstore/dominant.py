"""主力合约别名解析模块

支持将品种主力代号（如 TL01）解析为具体合约（如 TL2502）。
TL01 = TL（品种）+ 01（主力标记），通过 market-specs 的 get_dominant() 映射。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from market_specs.dominant import get_dominant as _ms_get_dominant

# 主力标记代号
DOMINANT_CODE = "01"


@dataclass
class DominantAlias:
    """解析后的主力别名对象。"""
    raw: str                    # 原始输入，如 "TL01"
    symbol: str                  # 品种代码，如 "TL"
    code: str                    # 别名代码，如 "01"
    is_dominant: bool            # 是否为 01（主力）标记


def is_dominant(code: str) -> bool:
    """
    判断代号是否为主力标记。

    Args:
        code: 别名代码，如 "01"

    Returns:
        True if code == DOMINANT_CODE ("01")
    """
    return code == DOMINANT_CODE


def parse_alias(raw: str) -> Optional[DominantAlias]:
    """
    将原始字符串解析为 DominantAlias。

    规则：前 2~4 个字母为品种，后面紧跟 2 位数字为代号。
    例如：
        "TL01"  -> symbol="TL", code="01", is_dominant=True
        "TL02"  -> symbol="TL", code="02", is_dominant=False
        "IF2403" -> None（不符合别名格式，视为具体合约）

    Args:
        raw: 原始字符串，如 "TL01"

    Returns:
        DominantAlias if pattern matches, else None
    """
    if not raw or len(raw) < 3:
        return None

    # 提取品种部分（头部字母）+ 代号部分（尾部数字）
    # 品种: 1~4 个字母（保留大写），支持单字母品种如 T
    m = re.match(r'^([A-Za-z]{1,4})(\d{2})$', raw)
    if not m:
        return None

    symbol = m.group(1).upper()
    code = m.group(2)  # keep as-is, "01" not "1"

    return DominantAlias(
        raw=raw,
        symbol=symbol,
        code=code,
        is_dominant=is_dominant(code),
    )


def resolve_contract(
    contract: str,
    trade_date: str,
) -> str:
    """
    解析合约代码：如果是主力别名，则展开为具体合约。

    Args:
        contract: 合约代码或主力别名，如 "TL2502" 或 "TL01"
        trade_date: 交易日期，格式 YYYY-MM-DD

    Returns:
        具体合约代码，如 "TL2502"
    """
    alias = parse_alias(contract)
    if alias is None or not alias.is_dominant:
        return contract  # 不是主力别名，直接返回

    # 通过 market-specs 查询主力合约
    dominant_contract = _ms_get_dominant(alias.symbol, trade_date)
    if dominant_contract is None:
        raise ValueError(
            f"未找到 {alias.symbol} 在 {trade_date} 的主力合约"
        )
    return dominant_contract


def _normalize_date_str(d: str) -> str:
    """将各种日期格式统一为 YYYY-MM-DD。"""
    d = d.strip()
    if len(d) == 8 and d.isdigit():  # YYYYMMDD
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    elif len(d) == 10 and d[4] == "-" and d[7] == "-":  # YYYY-MM-DD
        return d
    else:
        # fallback: try datetime parse
        import datetime
        try:
            dt = datetime.datetime.strptime(d, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return d
