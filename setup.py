from distutils.core import setup

from setuptools import find_packages

setup(
    name='process-data-from-lake',
    version='1.0.11',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['process-data-from-lake=data_lake.process_data:main']
    },
    include_package_data=True
)
