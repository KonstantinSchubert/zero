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
        "b2==3.2.1",
        "pyyaml>=4.2b1",
        "portalocker==1.2.1",
        "pika==1.0.1",
        # sudo apt install rabbitmq-server
        # sudo service rabbitmq-server status
    ],
    include_package_data=True,
    zip_safe=True,
    entry_points={
        "console_scripts": [
            "zero = zero.main:main",
            "debug-delete-everything = zero.main:reset_all",
        ]
    },
)
