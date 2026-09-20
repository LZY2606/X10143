from setuptools import find_packages, setup

setup(
    name="leasedebug",
    version="0.1.0",
    description="Deterministic distributed-lease debugging workbench",
    packages=find_packages(include=["leasedebug", "leasedebug.*"]),
    include_package_data=True,
    package_data={"leasedebug": ["static/*"]},
    python_requires=">=3.9",
    extras_require={"test": ["pytest>=7"]},
    entry_points={"console_scripts": ["leasedebug=leasedebug.app:main"]},
)
