# README

## 1 What's this:

Download uniswap pool event log, and convert it to different format.

Demeter-fetch support download from the following source:

* RPC: query data from rpc interface of any Ethereum like chain node.
* Google BigQuery: just support Ethereum and Polygon, and cost a little dollar, but will fetch faster than rpc

Usually, query a pool logs of a day form bigquery will cost 10 seconds. while query from node will cause several minutes.

Demeter-fetch support export data in following type:

* raw: Original event log. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.raw.csv)
* minute: process uniswap data and resample it to minute, [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv). Demeter use data in this type. 
* tick: process uniswap data, each log will be decoded and listed. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.tick.csv)

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

> note: If you have network issues on google, set proper proxy before download data.

### 2.2 Prepare rpc

To use node, you can get sign up a data provider account like infura, quicknode, alchemy. or setup your node to short the request delay. If you have trouble on connection, demeter also provide proxy configuration.

### 2.3 Install

demeter-fetch is not avaliable on Pypi, you need to clone this repo, and run locally. then install dependency.

### 2.4 Download

Create a target folder to store downloaded files, then prepare a config.toml file according to [config-sample.toml](config-sample.toml)

then execute:

```shell
cd demeter-fetch
python main.py config.toml

```

## 3 File format

The downloaded data is grouped by date, that is, one file per day. 

The format of the file name is "[chain name]-[Pool/token contract address]-[date].[file type].csv". e.g. polygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv

To prevent download failure, demeter-fetch will download all files first, then convert raw files to minute/tick files. So after the download is completed, the target folder will save two kinds of files, .raw.csv and .minute.csv/.tick.csv.


* Raw file is original transaction event log, one row for an event log, [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.raw.csv).
* Minute file is used in demeter. In this file, event logs are abstracted to market data, such as price, total liquidity, apy etc. For the convenience of backtesting, data is resampled minutely. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv). 
* Like minute file, in tick file, event logs are also abstracted to market data, but data will not be resampled. so one row for an event log. Some transaction information such as block number and transaction hash is also kept. It is often used for market analysis. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.tick.csv)


## 4 Release note

You can find it [here](release_note.md)



