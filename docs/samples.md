# Samples

You can download different data via config. 

## 1. For back testing with demeter

### Download from BigQuery

If you want to backtest via demeter, you should download minute data. Here is a basic example to download minute data from bigquery.

```toml
[from]
chain = "ethereum"
datasource = "big_query"
dapp_type = "uniswap"
http_proxy = "http://127.0.0.1:7890" # set proxy if cannot connect google service

[from.uniswap]
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

[from.big_query]
start = "2023-11-25"
end = "2023-11-25"
auth_file = "your_google_auth_json" # google bigquery auth file

[to]
type = "minute"
save_path = "./sample" # save data to this folder
multi_process = false
```

Then run: 
```shell
demeter-fetch -c config_bigquery.toml 
```

It will generate the following output, line start with # is my comment.

```text
# Print config, so you can check it.
2023-11-30 23:06:14 Download config: {
    "from_config": {
        "chain": "ethereum",
        "data_source": "big_query",
        "dapp_type": "uniswap",
        "uniswap_config": {
            "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
        },
        "aave_config": null,
        "big_query": {
            "start": "2023-11-25",
            "end": "2023-11-25",
            "auth_file": "your google auth json"
        },
        "chifra_config": null,
        "rpc": null,
        "file": null,
        "http_proxy": "http://127.0.0.1:7890"
    },
    "to_config": {
        "type": "tick",
        "save_path": "./sample",
        "multi_process": false,
        "skip_existed": false,
        "to_file_list": {}
    }
}
# Start download, in this step, demeter-fetch will query event logs from Google BigQuery, and save them to .raw.csv files.
2023-11-30 23:06:14 Will generate 1 files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:08<00:00,  8.68s/it]

2023-11-30 23:06:23 Download finish

# In this step, .raw.csv files will be converted to .minute.csv files.
2023-11-30 23:06:23 Start generate 1 files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:02<00:00,  2.18s/it]
Process finished with exit code 0

```

At last, you will find the following files in ./sample:

```text
ethereum-0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640-2023-11-25.raw.csv
ethereum-0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640-2023-11-25.minute.csv
```

### Download from rpc

Just modify you config

```toml
[from]
chain = "ethereum"
datasource = "rpc" # change datasource from big_query to rpc
dapp_type = "uniswap"

[from.uniswap]
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

[from.rpc] # remove from.big_query and set this section
end_point = "http://127.0.0.1:8545"
start = "2023-11-25"
end = "2023-11-25"
ignore_position_id = true

[to]
type = "minute"
save_path = "./sample"
multi_process = false
```


### download from chifra

You should first export files from chifra, including pool logs, then convert with demeter-fetch. 

```toml
[from]
chain = "ethereum"
datasource = "chifra"
dapp_type = "uniswap"

[from.uniswap]
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

[from.chifra] # add this secion
file_path = "./sample/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640_2023-11-10.raw.csv"

[to]
type = "true"
save_path = "./sample"
multi_process = false
```

> If you want to skip existed files. set ```skip_existed = true``` in "to" section.

### convert existing raw files

If you already have .raw.csv files and wish to convert them to .minute.csv files, you can do this:

```toml
[from]
chain = "ethereum"
datasource = "file"
dapp_type = "uniswap"

[from.uniswap]
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

[from.file]# add this secion
folder = "./sameple"

[to]
type = "true"
save_path = "./sample"
multi_process = false
```

## 2. For defi-research

If you want to do some research on uniswap, you need tick files. Just set "type" in "to" section to "tick"

```toml
[from]
chain = "ethereum"
datasource = "big_query"
dapp_type = "uniswap"
http_proxy = "http://127.0.0.1:7890" 

[from.uniswap]
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

[from.big_query]
start = "2023-11-25"
end = "2023-11-25"
auth_file = "your_google_auth_json"

[to]
type = "tick" # Update this section
save_path = "./sample" 
multi_process = false
```

Then you will get tick files in to folder

```text
ethereum-0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640-2023-11-25.tick.csv

```

Tick file contains position id, it is kept in logs of uniswap proxy contract. This will cause extra work in downloading, so if you don't need position id, you can set ```ignore_position_id=true```, 
The three souces should be configed like this:

```shell
[from.big_query] # No extra config.
start = "2023-11-25"
end = "2023-11-25"
auth_file = "your_google_auth_json"

[from.rpc]
end_point = "https://polygon-mainnet.infura.io/v3/your_api_key"
start = "2023-11-20"
end = "2023-11-20"
ignore_position_id = true # Set this to true

[from.chifra]
file_path = "./sample/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640_2023-11-10.raw.csv"
ignore_position_id = true  # Set this to true

```

And if you do need position id, you should set ```ignore_position_id=false```, you can just remove this config, as it's default value is false.

* For rpc, demeter-fetch will spend extra time to download proxy logs, and match them with pool logs.
* For chifra, you have export proxy logs, and set its path.

```shell
[from.big_query] # No extra config.
start = "2023-11-25"
end = "2023-11-25"
auth_file = "your_google_auth_json"

[from.rpc]
end_point = "https://polygon-mainnet.infura.io/v3/your_api_key"
start = "2023-11-20"
end = "2023-11-20"
ignore_position_id = false # Set this to true,

[from.chifra]
file_path = "./sample/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640_2023-11-10.raw.csv"
proxy_file_path = "./sample/0xc36442b4a4522e871399cd717abdd847ab11fe88_2023-11-10.raw.csv"
ignore_position_id = true  # Set this to true

```












