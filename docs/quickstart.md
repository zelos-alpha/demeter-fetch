# Quick start

## 1. System requirement

* OS: linux. (Windows and mac are not fully tested)
* Python >= 3.10

## 2. Prepare data source

You can choose a data source that suits you in the following section. 

### 2.1 big query

If you want to download via BigQuery, you should prepare an account and environment.

1. Sign up a Google account. and then access [Google cloud](https://console.cloud.google.com) to register Google cloud
   platform.
2. Create a new project, [https://console.cloud.google.com/projectcreate](https://console.cloud.google.com/projectcreate)
3. Create a [service account](https://console.cloud.google.com/iam-admin/serviceaccounts)
4. Manage keys of the service account just created, create a new key with type of JSON, 
then you will get a JSON file which will be used to permit demeter-fetch to access google cloud.

> If you want to try your own query, you can visit [Google cloud console](https://console.cloud.google.com/bigquery). Or query via api, 
> whose tutorial can be found [here](https://cloud.google.com/bigquery/docs/reference/libraries)
>
> Here is an example of query from blocks.
>
> ```sql
> select * from bigquery-public-data.crypto_ethereum.blocks where timestamp="2015-07-30 15:26:28"
> ```

### 2.2 RPC

If you want to query from RPC interface of evm chains, you should prepare a node. Or query from an RPC provider like
infura, quicknode or alchemy.
Setting up your own node will reduce network transmission delays, thus reducing download times.
If you have trouble on connection, demeter-fetch also provides proxy configuration.

### 2.3 Chifra

If you have your own node, you can try [Chifra](https://trueblocks.io/chifra/introduction/) from trueblocks.
After [installing](https://trueblocks.io/docs/install/install-core/), you can indexing chain data with ```chifra scrape```. 
When sync finished, chifra support exporting chain data like blocks, transactions, logs, balances.

## 3 Install and run

demeter-fetch is available on Pypi, you can install by

```shell
pip install demeter-fetch
```

Then prepare a configration file according to [config-sample.toml](../config-sample.toml)

At last, execute:

```shell
demeter-fetch -c config.aave.toml
```

If you want to use pre-release version, you can clone this repo, and run locally.

```shell
git clone https://github.com/zelos-alpha/demeter-fetch.git
cd demeter-fetch
python main.py -c config.aave.toml
```

Using different configuration files, demeter can download different types of data. 
Visit [samples](samples.md) section for more examples