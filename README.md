# FactorStore

轻量级多频率因子 Parquet 存储与查询引擎，专为量化策略开发设计。

## 核心特性

- **极简接口**：5 个方法搞定因子的存/读/查/删
- **自动化处理**：时间戳类型转换、列名前缀、目录创建/清理全自动
- **时间戳对齐保障**：同一分区下所有因子文件行数和 ts 列强制一致
- **高效存储**：Parquet 列式存储 + zstd 压缩
- **多频率支持**：tick / 1min / 5min，互不干扰

## 安装

```bash
# conda 环境
conda activate quantdev
pip install -e .
```

## 快速上手

```python
import datetime
import polars as pl
from factorstore import FactorStore

# 初始化（默认路径 ~/data/factor_store）
store = FactorStore()

# 构造因子数据
ts = [datetime.datetime(2026, 1, 5, 9, 30, 0) + datetime.timedelta(milliseconds=500 * i) for i in range(5)]
df = pl.DataFrame({
    "ts": ts,
    "mid": [5230.5, 5231.0, 5230.8, 5231.2, 5230.9],
    "volume": [100.0, 150.0, 80.0, 200.0, 120.0],
})

# 保存
store.save_factor("TL2603", "20260105", "MdDMU_v0", df, "tick")

# 读取
result = store.load_factors("TL2603", "20260105", ["MdDMU_v0"], "tick")
print(result)
```

## 目录结构

```
~/data/factor_store/
├── tick/
│   └── TL2603/
│       └── 20260105/
│           ├── MdDMU_v0.parquet
│           └── NetBuyDMU_v1.parquet
├── 1min/
└── 5min/
```

## API

| 方法 | 说明 |
|------|------|
| `save_factor(contract, trade_date, factor_name, df, frequency)` | 保存因子，自动类型转换、加前缀、对齐校验 |
| `load_factors(contract, trade_date, factor_names, frequency)` | 读取并横向拼接多个因子 |
| `list_factors(contract, trade_date, frequency)` | 查询可用因子列表 |
| `exists(contract, trade_date, factor_name, frequency)` | 检查因子是否存在 |
| `delete_factor(contract, trade_date, factor_name, frequency)` | 删除因子文件并清理空目录 |

## 设计要点

- **自动列名前缀**：保存时自动给因子列加 `{factor_name}_` 前缀，避免多因子拼接时列名冲突
- **时间戳对齐校验**：同一分区下写入第二个因子时，自动校验行数和 ts 列与已有因子一致
- **中文异常信息**：所有错误提示均为中文，方便定位问题
- **zstd 压缩**：Parquet 文件默认使用 zstd 压缩，兼顾压缩比和速度

## 依赖

- Python >= 3.10
- Polars >= 1.0

## 后续改进方案

- [ ] **引入 DuckDB 支持跨分区 SQL 查询**：当前实现基于 Polars 精确路径读取，适合单品种单日场景。后续可引入 DuckDB，支持 `SELECT * FROM read_parquet('root/**/*.parquet') WHERE ...` 风格的跨品种/跨日期批量查询，并可考虑恢复 Hive 风格分区目录（`frequency=tick/contract=TL2603/date=20260105/`）以启用 DuckDB 自动分区裁剪
- [ ] **支持更多频率**：如 15min、30min、1h 等，只需扩展 `VALID_FREQUENCIES` 集合
- [ ] **多进程并发写入优化**：当前依赖 Parquet 原子写入保证安全，后续可加文件锁提升并发可靠性
- [ ] **因子元数据管理**：记录因子的创建时间、版本、计算参数等元信息
- [ ] **数据完整性校验**：增加 checksum 验证，防止文件损坏

## License

MIT
