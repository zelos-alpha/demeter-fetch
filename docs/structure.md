## 5 project structure

```text
demeter_fetch
|-- __init__.py
|-- common
|   |-- __init__.py
|   |-- _typing.py
|   |-- nodes.py
|   `-- utils.py
|-- core
|   |-- __init__.py
|   |-- commands.py
|   |-- config.py
|   |-- downloader.py
|   `-- engine.py
|-- main.py
|-- processor_aave
|   |-- __init__.py
|   |-- aave_utils.py
|   |-- minute.py
|   `-- tick.py
|-- processor_uniswap
|   |-- __init__.py
|   |-- minute.py
|   |-- position.py
|   |-- tick.py
|   `-- uniswap_utils.py
|-- sources
|   |-- __init__.py
|   |-- big_query.py
|   |-- big_query_utils.py
|   |-- chifra.py
|   |-- chifra_utils.py
|   |-- rpc.py
|   |-- rpc_utils.py
|   |-- source_core.py
|   `-- source_utils.py
`-- tools
    |-- __init__.py
    `-- time_tools.py

```