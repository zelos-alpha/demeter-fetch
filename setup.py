from setuptools import setup, find_packages

setup(
    name='demeter-fetch',
    version='1.3.3',
    packages=find_packages(exclude=["tests", "tests.*", "samples", "samples.*"]),
    url='',
    license='MIT',
    author='zelos research',
    author_email='zelos@antalpha.com',
    description='demeter fetch tool',
    python_requires='>=3.11',
    install_requires=[
        "numpy>=1.26.3",
        "pandas>=2.2.1",
        "protobuf>=4.25.2",
        "Requests>=2.31.0",
        "toml>=0.10.2",
        "tqdm>=4.66.2",
        "google-cloud-bigquery>=3.19.0",
        "db-dtypes>=1.2.0",
        "argparse>=1.4.0",
        "sqlitedict>=2.1.1",
    ],
    entry_points={
        'console_scripts': [
            'demeter-fetch = demeter_fetch.main:main',
        ],
    }
)
