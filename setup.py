# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name="zero",
    version="0.0.1",
    description="Unlimited local storage",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    keywords="",
    author="Konstantin Schubert",
    packages=find_packages(),
    install_requires=[
        "fusepy==2.0.4",
        "b2==1.1.0",
        "pyyaml>=4.2b1",
        "portalocker==1.2.1",
    ],
    include_package_data=True,
    zip_safe=True,
    entry_points={
        "console_scripts": [
            "zero-fuse = zero.main:fuse_main",
            "zero-worker = zero.main:worker_main",
            "debug-delete-everything = zero.main:reset_all",
        ]
    },
)
