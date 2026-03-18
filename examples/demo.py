"""FactorStore 使用案例演示。"""

import datetime
import pandas as pd
from factorstore import FactorStore

# ========== 1. 初始化 ==========
# 使用默认路径 ~/data/factor_store（也可传入自定义绝对路径）
store = FactorStore()
print(f"数据根目录: {store.root_path}\n")

# ========== 2. 构造因子数据 ==========
# 模拟 TL2603 合约 2026-01-05 的 tick 数据（10 个 tick）
ts = [datetime.datetime(2026, 1, 5, 9, 30, 0) + datetime.timedelta(milliseconds=500 * i) for i in range(10)]

# 因子 1: MdDMU_v0 —— 行情类因子（Pandas DataFrame，时间为 index）
df_md = pd.DataFrame({
    "mid": [5230.5, 5231.0, 5230.8, 5231.2, 5230.9, 5231.5, 5232.0, 5231.8, 5232.3, 5232.1],
    "volume": [100.0, 150.0, 80.0, 200.0, 120.0, 180.0, 90.0, 160.0, 110.0, 140.0],
}, index=ts)

# 因子 2: NetBuyDMU_v1 —— 资金流因子
df_net = pd.DataFrame({
    "net_small_order": [10.0, -5.0, 8.0, -3.0, 12.0, -7.0, 15.0, -2.0, 9.0, -4.0],
    "net_big_order": [50.0, 30.0, -20.0, 80.0, -10.0, 60.0, 40.0, -30.0, 70.0, 20.0],
}, index=ts)

# ========== 3. 保存因子 ==========
store.save_factor("TL2603", "20260105", "MdDMU_v0", df_md, "tick")
store.save_factor("TL2603", "20260105", "NetBuyDMU_v1", df_net, "tick")
print("✅ 两个因子已保存\n")

# ========== 4. 查询可用因子 ==========
factors = store.list_factors("TL2603", "20260105", "tick")
print(f"可用因子: {factors}")

# ========== 5. 检查因子是否存在 ==========
print(f"MdDMU_v0 存在: {store.exists('TL2603', '20260105', 'MdDMU_v0', 'tick')}")
print(f"Foo_v0 存在: {store.exists('TL2603', '20260105', 'Foo_v0', 'tick')}\n")

# ========== 6. 读取单个因子 ==========
result_single = store.load_factors("TL2603", "20260105", ["MdDMU_v0"], "tick")
print("单因子读取:")
print(result_single)
print()

# ========== 7. 多因子横向拼接读取 ==========
result_multi = store.load_factors("TL2603", "20260105", ["MdDMU_v0", "NetBuyDMU_v1"], "tick")
print("多因子拼接读取:")
print(result_multi)
print()

# ========== 8. 删除因子 ==========
store.delete_factor("TL2603", "20260105", "NetBuyDMU_v1", "tick")
print("✅ NetBuyDMU_v1 已删除")
print(f"删除后可用因子: {store.list_factors('TL2603', '20260105', 'tick')}\n")

# ========== 9. 错误处理演示 ==========
print("--- 错误处理演示 ---")

# 非法频率
try:
    store.save_factor("TL2603", "20260105", "test", df_md, "15min")
except ValueError as e:
    print(f"ValueError: {e}")

# 缺少 ts 列（Polars DataFrame 无 index 也无 ts 列）
try:
    import polars as pl
    bad_df = pl.DataFrame({"value": [1.0, 2.0]})
    store.save_factor("TL2603", "20260105", "test", bad_df, "tick")
except ValueError as e:
    print(f"ValueError: {e}")

# 加载不存在的因子
try:
    store.load_factors("TL2603", "20260105", ["NotExist_v0"], "tick")
except FileNotFoundError as e:
    print(f"FileNotFoundError: {e}")

print("\n🎉 演示完成")
