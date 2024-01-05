# Quick start

## 1. Prepare data source

### 1.1 big query

If you want to download via BigQuery, you should prepare an account and environment.

1. Sign up a Google account. and then access [Google cloud](https://console.cloud.google.com) to register Google cloud
   platform.
2. Apply an api key, which will be used in config file of demeter-fetch
3. If you want to try out your own query, try a query here: https://console.cloud.google.com/bigquery. If you want to
   query by api, you can install BigQuery API Client Libraries library. follow the tutorial
   on [official document site](https://cloud.google.com/bigquery/docs/reference/libraries)

Here is an example of query from blocks.

```sql
select * from bigquery-public-data.crypto_ethereum.blocks where timestamp="2015-07-30 15:26:28"
```

### 1.2 Prepare RPC

If you want to query from RPC interface of evm chains, you should find a node. Or query from an RPC provider like
infura, quicknode, alchemy.
Set up your own node will reduce network transmission delays, thus reducing download times.
If you have trouble on connection, demeter-fetch also provides a configuration on proxy.

### 1.3 Prepare Chifra

If you have your own node, you can try Chifra from trueblocks.
After [installing](https://trueblocks.io/docs/install/install-core/).
You can start indexing chain data with ```chifra scrape```. After sync finished, you can export chain data like blocks,
transactions, logs, balances.

## 2 Install and run

demeter-fetch is available on Pypi, you can install by

```shell
pip install demeter-fetch
```

Then prepare a configration file according to [config-sample.toml](../config-sample.toml)

At last, execute:

```shell
demeter-fetch -c config.toml
```

If you want to run the latest version, you can clone this repo, and run locally.

```shell
git clone https://github.com/zelos-alpha/demeter-fetch.git
cd demeter-fetch
python main.py -c config.toml
```

More samples are here: [samples](samples.md)