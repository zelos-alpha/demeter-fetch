## 2 How to use

### 2.1 Prepare big query

To use bigquery, you should prepare account and environment.

1. Sign up a google account. and then access [google cloud](https://console.cloud.google.com) to register google cloud platform.
2. Apply an api key, and install library. follow the tutorial on [official document site](https://cloud.google.com/bigquery/docs/reference/libraries)
3. Try query here: https://console.cloud.google.com/bigquery. Chain data is public, no extra authority is needed.

In BigQuery, you can query chain data with correct id and table name. the query interface is compatible sql. You can try with this sql

```sql
select * from bigquery-public-data.crypto_ethereum.blocks where timestamp="2015-07-30 15:26:28"
```

> note: If you have network issues on Google, set proper proxy before download data.

### 2.2 Prepare RPC

To use node, you can get sign up a data provider account like infura, quicknode, alchemy. or setup your node to short the request delay. If you have trouble on connection, demeter also provide proxy configuration.

### 2.3 Prepare Chifra

You should install as doc [TrueBlock install](https://trueblocks.io/docs/install/install-core/)  
Remember it only support Linux and Mac system.  
And if you have trouble, you can find result at [install troubleshooting](https://trueblocks.io/docs/install/install-troubleshooting/)

### 2.3 Install

demeter-fetch is not available on Pypi, you need to clone this repo, and run locally. then install dependency.

### 2.4 Download

Create a target folder to store downloaded files, then prepare a config.toml file according to [config-sample.toml](config-sample.toml)
* from.uniswap/from.aave from which dapp get event log
* from.big_query/from.rpc/from.file/from.chifra data source configure

then execute:

```shell
cd demeter-fetch
python main.py config.toml

```

More samples are here: [samples](docs/samples.md)