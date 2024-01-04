from setuptools import setup, find_packages

setup(
    name='demeter-fetch',
    version='0.2.0',
    packages=find_packages(exclude=["tests", "tests.*", "samples", "samples.*"]),
    url='',
    license='MIT',
    author='zelos research',
    author_email='zelos@antalpha.com',
    description='demeter fetch tool',
    python_requires='>=3.10',
    install_requires=[
        "pandas>=1.4.4",
        "protobuf>=4.21.9",
        "Requests>=2.28.0",
        "toml>=0.10.2",
        "tqdm>=4.65.0",
        "google-cloud-bigquery>=3.11",
        "db-dtypes>=1.1.1",
        "argparse>=1.4.0",
    ],
    entry_points={
        'console_scripts': [
            'demeter-fetch = demeter_fetch.main:main',
        ],
    }
)
