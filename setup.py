from setuptools import setup, find_packages
setup(
    name="leasedebug",
    version="0.1.0",
    description="分布式租约时序重放调试台（虚拟时钟模拟）",
    package_dir={"": "src"},
    packages=find_packages("src"),
    include_package_data=True,
    package_data={"leasedebug": ["static/*"]},
    python_requires=">=3.9",
    extras_require={"test": ["pytest>=7"]},
    entry_points={"console_scripts": ["leasedebug=leasedebug.__main__:main"]},
)
