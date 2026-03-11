from setuptools import setup, find_packages

setup(
    name="factorstore",
    version="0.1.0",
    description="轻量级多频率因子 Parquet 存储与查询引擎",
    packages=find_packages(exclude=["tests", "tests.*", "examples"]),
    python_requires=">=3.10",
    install_requires=[
        "polars>=1.0",
        "pyarrow>=14.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0",
            "hypothesis>=6.100",
        ],
    },
)
