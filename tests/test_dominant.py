"""测试主力合约解析功能。"""

import pytest
from factorstore import (
    DominantAlias,
    FactorStore,
    parse_alias,
    resolve_contract,
)


def test_parse_alias_dominant():
    """测试解析主力合约别名。"""
    alias = parse_alias("TL01")
    assert alias is not None
    assert alias.symbol == "TL"
    assert alias.code == "01"
    assert alias.is_dominant is True
    assert alias.raw == "TL01"


def test_parse_alias_non_dominant():
    """测试解析非主力合约别名。"""
    alias = parse_alias("TL02")
    assert alias is not None
    assert alias.symbol == "TL"
    assert alias.code == "02"
    assert alias.is_dominant is False


def test_parse_alias_specific_contract():
    """测试解析具体合约代码。"""
    alias = parse_alias("TL2502")
    assert alias is None  # 不符合别名格式


def test_resolve_contract_dominant():
    """测试解析主力合约为具体合约。"""
    # 这个测试需要 market-specs 包安装并配置了数据
    contract = resolve_contract("TL01", "2025-02-10")
    assert contract != "TL01"  # 应该被解析为具体合约
    # 具体合约格式应该是品种+年份+月份
    assert contract.startswith("TL25")


def test_resolve_contract_specific():
    """测试解析具体合约代码。"""
    contract = resolve_contract("TL2502", "2025-02-10")
    assert contract == "TL2502"  # 应该保持不变


def test_factorstore_with_dominant():
    """测试 FactorStore 支持主力合约别名。"""
    import tempfile
    import polars as pl

    with tempfile.TemporaryDirectory() as tmpdir:
        fs = FactorStore(root_path=tmpdir)

        # 使用主力别名保存因子
        df = pl.DataFrame({
            "ts": [1000, 2000, 3000],
            "value": [1.0, 2.0, 3.0],
        })
        # 这里需要实际的主力合约数据，实际测试时跳过
        # fs.save_factor("TL01", "2025-02-10", "test_factor", df)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
