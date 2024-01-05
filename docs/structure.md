## 5 project structure

```text
.
├── LICENSE
├── README.md
├── auth
│   └── google_bigquery_auth.json
├── config-sample.toml            sample config
├── demeter_fetch
│   ├── __init__.py
│   ├── _typing.py                typing defination
│   ├── aave_downloader.py
│   ├── constants.py              constants
│   ├── general_downloader.py     general download code
│   ├── processor_aave
│   │   ├── __init__.py
│   │   ├── aave_utils.py
│   │   ├── minute.py
│   │   └── tick.py
│   ├── processor_uniswap         downloaded raw file to uniswap tick/minute file
│   │   ├── __init__.py
│   │   ├── minute.py
│   │   ├── tick.py
│   │   └── uniswap_utils.py
│   ├── source_big_query          fetch data from bigquery
│   │   ├── __init__.py
│   │   ├── aave.py
│   │   ├── big_query_utils.py
│   │   └── uniswap.py
│   ├── source_chifra             fetch data from chifra
│   │   ├── __init__.py
│   │   ├── chifra_utils.py
│   │   └── uniswap.py
│   ├── source_file               deal with exist raw file
│   │   ├── __init__.py
│   │   └── common.py
│   ├── source_rpc                fetch data from rpc
│   │   ├── __init__.py
│   │   ├── aave.py
│   │   ├── eth_rpc_client.py
│   │   └── uniswap.py
│   ├── uniswap_downloader.py      uniswap downloader main logic
│   └── utils.py                   utility
├── main.py                        project enter point
├── release_note.md
├── requirements.txt               project dependence
├── sample                         sample data
├── tests                          testcase 
└── workflow.png
```