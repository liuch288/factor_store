# FactorStore

轻量级多频率因子 Parquet 存储与查询引擎，专为量化策略开发设计。

## 核心特性

- **极简接口**：5 个方法搞定因子的存/读/查/删
- **主力合约支持**：自动解析主力合约别名（如 `TL01`）为具体合约（如 `TL2502`）
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
import pandas as pd
from factorstore import FactorStore

# 初始化（默认路径 ~/data/factor_store）
store = FactorStore()

# 构造因子数据（时间为 index，列为因子值）
ts = [datetime.datetime(2026, 1, 5, 9, 30, 0) + datetime.timedelta(milliseconds=500 * i) for i in range(5)]
df = pd.DataFrame({
    "mid": [5230.5, 5231.0, 5230.8, 5231.2, 5230.9],
    "volume": [100.0, 150.0, 80.0, 200.0, 120.0],
}, index=ts)

# 保存（trade_date 支持 str 或 datetime.date）
store.save_factor("TL2603", "2026-01-05", "MdDMU_v0", df, "tick")

# 读取（返回 Pandas DataFrame，时间为 index）
result = store.load_factors("TL2603", datetime.date(2026, 1, 5), ["MdDMU_v0"], "tick")
print(result)
```

## 目录结构

```
~/data/factor_store/
├── tick/
│   └── TL2603/
│       └── 2026-01-05/
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

## 主力合约支持

FactorStore 支持主力合约别名查询，自动将 `TL01`、`IF01` 等主力标记解析为具体合约。

```python
from factorstore import FactorStore, parse_alias, resolve_contract

# 解析主力别名
alias = parse_alias("TL01")
print(alias.symbol)   # "TL"
print(alias.code)     # "01"
print(alias.is_dominant)  # True

# 自动解析为具体合约
contract = resolve_contract("TL01", "2025-02-10")
print(contract)  # "TL2502"（实际主力合约）

# 在 FactorStore 中直接使用主力别名
store = FactorStore()
store.save_factor("TL01", "2025-02-10", "MdDMU", df)  # 自动解析为 TL2502
result = store.load_factors("TL01", "2025-02-10", ["MdDMU"])  # 同样自动解析
```

**主力别名格式**：品种代码 + `01`（主力标记）
- `TL01` = 天然橡胶主力
- `IF01` = 沪深300股指主力
- `TS01` = 中证1000股指主力

解析逻辑：
- 2-4 位字母开头 + 2 位数字结尾 = 视为别名
- 后缀 `01` = 主力合约，通过 `market-specs` 包查询具体合约
- 其他格式（如 `TL2502`）= 视为具体合约，直接使用

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

## 更新记录

### v0.1.1

- `trade_date` 参数支持 `datetime.date` 和 `str` 两种类型输入
- 日期格式自动规范化为 `YYYY-MM-DD`，支持 `YYYYMMDD`、`YYYY-MM-DD`、`datetime.date`、`datetime.datetime` 四种输入格式
- 所有涉及日期的接口（`save_factor`、`load_factors`、`list_factors`、`exists`、`delete_factor`）统一处理
- `save_factor` 支持 Pandas DataFrame 以时间为 index 直接传入，自动将 index 转为 `ts` 列存储
- `load_factors` 返回 Pandas DataFrame 时自动将 `ts` 列设为 index
- `load_factors` 多因子拼接时增加 ts 列对齐校验

### v0.1.0

- 初始版本
- 支持因子的存储、读取、查询、删除
- 支持 tick / 1min / 5min 三种频率
- 时间戳对齐校验、自动类型转换、列名前缀
- zstd 压缩存储
