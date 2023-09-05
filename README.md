# README

## What's this:

Download uniswap pool event log, and convert it to different format for demeter.

Demeter-fetch support download from the following source:

* RPC: query data from rpc interface of any Ethereum like chain node.
* Google BigQuery: just support Ethereum and Polygon, But will fetch faster than rpc

Demeter-fetch support export data in following type:

* raw: Original event log. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.raw.csv)
* minute: process uniswap data and resample it to
  minute, [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv)
* tick: process uniswap data, each log will be decoded and
  listed. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.tick.csv)

# how to use

Prepare a config.toml file according to [config-sample.toml](config-sample.toml)

then execute:

```shell
python main.py config.toml

```

# todo list

* rpc download support timeout parameter, and will retry if network is unstable.

# release note

v0.1.4 (2023-9-?)

* support aave
* add config: 
  * skip_existed in "to" section.
  * dapp_type in "From" section.
* [breaking change] address of uniswap pool is listed in from.uniswap



