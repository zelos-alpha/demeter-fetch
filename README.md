# README

## What's this:

Download uniswap pool event log, and convert it to different format for demeter.

Demeter-fetch support download from the following source:

* RPC: query data from rpc interface of any Ethereum like chain node.
* Google BigQuery: just support Ethereum and Polygon, and cost a little dollar, but will fetch faster than rpc

Usually, query a pool logs of a day form bigquery will cost 10 seconds. while query from node will cause several minutes.

Demeter-fetch support export data in following type:

* raw: Original event log. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.raw.csv)
* minute: process uniswap data and resample it to
  minute, [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv)
* tick: process uniswap data, each log will be decoded and
  listed. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.tick.csv)

## how to use

### prepare big query

To use bigquery, you should prepare account and environment.

1. Sign up a google account. and then access [google cloud](https://console.cloud.google.com) to register google cloud platform.
2. Apply an api key, and install library. follow the tutorial on [official document site](https://cloud.google.com/bigquery/docs/reference/libraries)
3. Try query here: https://console.cloud.google.com/bigquery. Chain data is public, no extra authority is needed.

In BigQuery, you can query chain data with correct id and table name. the query interface is compatible sql. You can try with this sql

```sql
select * from bigquery-public-data.crypto_ethereum.blocks where timestamp="2015-07-30 15:26:28"
```

> note: If you have network issues on google, set proper proxy before download data.

### prepare rpc

To use node, you can get sign up a data provider account like infura, quicknode, alchemy. or setup your node to short the request delay. If you have trouble on connection, demeter also provide proxy configuration.

### download

Prepare a config.toml file according to [config-sample.toml](config-sample.toml)

then execute:

```shell
python main.py config.toml

```

## release note

[release_note.md](release_note.md)



