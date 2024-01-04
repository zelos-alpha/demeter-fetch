# Samples

## 1. polygon bigquery download
config_bigquery.toml
```toml
[from]
chain = "polygon"
datasource = "rpc"
dapp_type = "uniswap"
http_proxy = "http://127.0.0.1:7890" # set proxy if cannot connect google service

[from.uniswap]
pool_address = "0x45dda9cb7c25131df268515131f647d726f50608"

[from.big_query]
start = "2023-11-25"
end = "2023-11-25"
auth_file = "your_google_auth_json" # google bigquery auth file

[to]
type = "tick"
save_path = "./sample"
multi_process = false
```
run script and get result:
```text
python main.py config_bigquery.toml 
2023-11-30 23:06:14 Download config: {
    "from_config": {
        "chain": "polygon",
        "data_source": "big_query",
        "dapp_type": "uniswap",
        "uniswap_config": {
            "pool_address": "0x45dda9cb7c25131df268515131f647d726f50608"
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
2023-11-30 23:06:14 Will generate 1 files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:08<00:00,  8.68s/it]


2023-11-30 23:06:23 Download finish
2023-11-30 23:06:23 Start generate 1 files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:02<00:00,  2.18s/it]
```


#### 2.5.3 polygon rpc download
config_rpc.toml
```toml
[from]
chain = "polygon"
datasource = "rpc"
dapp_type = "uniswap"

[from.uniswap]
pool_address = "0x9B08288C3Be4F62bbf8d1C20Ac9C5e6f9467d8B7"

[from.rpc]
end_point = "https://polygon-mainnet.infura.io/v3/your_api_key"
start = "2023-11-20"
end = "2023-11-20"
ignore_position_id = false

[to]
type = "tick"
save_path = "./sample"
multi_process = false
```

run script and get result:
```text
python main.py config_rpc.toml 
2023-11-30 22:43:34 Download config: {
    "from_config": {
        "chain": "polygon",
        "data_source": "rpc",
        "dapp_type": "uniswap",
        "uniswap_config": {
            "pool_address": "0x9b08288c3be4f62bbf8d1c20ac9c5e6f9467d8b7"
        },
        "aave_config": null,
        "big_query": null,
        "chifra_config": null,
        "rpc": {
            "end_point": "https://polygon-mainnet.infura.io/v3/your_api_key",
            "start": "2023-11-20",
            "end": "2023-11-20",
            "batch_size": 500,
            "auth_string": null,
            "keep_tmp_files": null,
            "ignore_position_id": false,
            "etherscan_api_key": null
        },
        "file": null,
        "http_proxy": null
    },
    "to_config": {
        "type": "tick",
        "save_path": "./sample-data",
        "multi_process": false,
        "skip_existed": false,
        "to_file_list": {}
    }
}
2023-11-30 22:43:34 Will generate 1 files
Download from 2023-11-20 to 2023-11-20
2023-11-30 22:43:35 Querying end timestamp, wait for 8 seconds to prevent max rate limit
2023-11-30 22:43:44 Will download from height 50151408 to 50191552
2023-11-30 22:43:44 Can not find a height cache, will generate one
100%|█████████████████████████████████████████████████████████████████████████████| 40145/40145 [08:01<00:00, 83.36it/s]
2023-11-30 22:51:46 Save block timestamp cache to ./sample-data/polygon_height_timestamp.pkl, length: 10660
2023-11-30 22:51:46 generate daily files
2023-11-30 22:51:46 Pool logs has downloaded, now will download proxy logs
2023-11-30 22:51:46 Height cache has loaded, length: 10660
100%|████████████████████████████████████████████████████████████████████████████| 40145/40145 [01:18<00:00, 510.79it/s]
2023-11-30 22:53:05 Save block timestamp cache to ./sample-data/polygon_height_timestamp.pkl, length: 10660
2023-11-30 22:53:05 start merge pool and proxy files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:03<00:00,  3.95s/it]


2023-11-30 22:53:09 Download finish
2023-11-30 22:53:09 Start generate 1 files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:06<00:00,  6.54s/it]
```




## 1 ethereum chifra(local) download
config_chifra_local.toml with local pool csv file and proxy csv file
```toml
[from]
chain = "ethereum"
datasource = "chifra"
dapp_type = "uniswap"

[from.uniswap]
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

[from.chifra]
file_path = "./sample/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640_2023-11-10.raw.csv"
ignore_position_id = false # Just for uniswap, if set to true, will not download uniswap proxy logs to get position_id. will save a lot of time
proxy_file_path = "./sample/0xc36442b4a4522e871399cd717abdd847ab11fe88_2023-11-10.raw.csv" # Just for uniswap, only required when ignore_position_id=False

[to]
type = "tick"
save_path = "./sample"
multi_process = false
skip_existed = true
```
run script:
```shell
python main.py config_chifra_local.toml
```
result:
```
2023-11-20 22:14:58 Download config: {
    "from_config": {
        "chain": "ethereum",
        "data_source": "chifra",
        "dapp_type": "uniswap",
        "uniswap_config": {
            "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
        },
        "aave_config": null,
        "big_query": null,
        "chifra_config": {
            "file_path": "./sample/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640_2023-11-10.raw.csv",
            "ignore_position_id": false,
            "proxy_file_path": "./sample/0xc36442b4a4522e871399cd717abdd847ab11fe88_2023-11-10.raw.csv",
            "start": null,
            "end": null,
            "etherscan_api_key": null
        },
        "rpc": null,
        "file": null,
        "http_proxy": null
    },
    "to_config": {
        "type": "tick",
        "save_path": "./sample",
        "multi_process": false,
        "skip_existed": true,
        "to_file_list": {}
    }
}
2023-11-20 22:14:58 Will generate 0 files
2023-11-20 22:14:58 Skip existed files, 0 files is exist, now will generate 0 files
2023-11-20 22:14:58 Loading csv file
2023-11-20 22:14:58 Process files
2023-11-20 22:14:59 Pool logs has downloaded, now will convert proxy logs
2023-11-20 22:14:59 loading proxy file
2023-11-20 22:14:59 Matching proxy log to pool logs, this may take a while
100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:04<00:00,  4.19s/it]
2023-11-20 22:15:04 Start to save files
2023-11-20 22:15:04 Saving daily files
100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  2.17it/s]
  0%|                                                                                             | 0/1 [00:00<?, ?it/s]

2023-11-20 22:15:04 Download finish
2023-11-20 22:15:04 Start generate 1 files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:08<00:00,  8.17s/it]
```

## 2. ethereum chifra(export) download
config_chifra_export.toml with local pool csv file and proxy csv file
```toml
[from]
chain = "ethereum"
datasource = "chifra"
dapp_type = "uniswap"
http_proxy = "http://127.0.0.1:7890"  # if cannot connect etherscan.io

[from.uniswap]
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

[from.chifra]
file_path = "./sample"
ignore_position_id = false
proxy_file_path = "./sample"
start = "2023-11-15"
end = "2023-11-15"
etherscan_api_key = "your_api_key" # must set

[to]
type = "tick"
save_path = "./sample"
multi_process = false
skip_existed = true
```

run script and result:
```text
python main.py config_chifra_export.toml 
  0%|                                                                                             | 0/1 [00:00<?, ?it/s]2023-11-30 14:30:28 Download config: {
    "from_config": {
        "chain": "ethereum",
        "data_source": "chifra",
        "dapp_type": "uniswap",
        "uniswap_config": {
            "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
        },
        "aave_config": null,
        "big_query": null,
        "chifra_config": {
            "file_path": "/sample",
            "ignore_position_id": false,
            "proxy_file_path": "/sample",
            "start": "2023-11-15",
            "end": "2023-11-15",
            "etherscan_api_key": "your_api_key"
        },
        "rpc": null,
        "file": null,
        "http_proxy": "http://127.0.0.1:7890"
    },
    "to_config": {
        "type": "tick",
        "save_path": "/sample",
        "multi_process": false,
        "skip_existed": true,
        "to_file_list": {}
    }
}
2023-11-30 14:30:28 Will generate 1 files
2023-11-30 14:30:28 Skip existed files, 0 files is exist, now will generate 1 files
2023-11-30 14:30:41 fetch start_height: 18573579, end_height: 18580726
PROG[26-12|14:31:03.240] [..................................................]  0%  4784/ 6847521 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:36<00:00, 36.11s/it]
  0%|                                                                                             | 0/1 [00:00<?, ?it/s]2023-11-30 14:31:15 fetch start_height: 18573579, end_height: 18580726
PROG[26-12|14:31:26.838] [..................................................]  0%  2665/ 2239034 0xc36442b4a4522e871399cd717abdd847ab11fe88
2023-11-30 14:31:28 Loading csv file
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:23<00:00, 23.23s/it]
2023-11-30 14:31:28 Process files
2023-11-30 14:31:28 Pool logs has downloaded, now will convert proxy logs
2023-11-30 14:31:28 loading proxy file
2023-11-30 14:31:28 Matching proxy log to pool logs, this may take a while
100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  1.58it/s]
2023-11-30 14:31:29 Start to save files
2023-11-30 14:31:29 Saving daily files


2023-11-30 14:31:29 Download finish
2023-11-30 14:31:29 Start generate 1 files
100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00, 10.47it/s]
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:01<00:00,  1.84s/it]
```



#### 2.5.5 convert raw file to tick/minute resample file
config_convert.toml
```toml
[from]
chain = "polygon"
datasource = "file"
dapp_type = "uniswap"

[from.uniswap]
pool_address = "0x45dda9cb7c25131df268515131f647d726f50608"

[from.file]
files = [
    "./sample/polygon-0x45dda9cb7c25131df268515131f647d726f50608-2023-11-25.raw.csv"
]
[to]
type = "minute" # minute or tick or raw
save_path = "./sample"
multi_process = false
```

run script and get result:
```text
python main.py config_convert.toml 
2023-11-30 23:13:53 Download config: {
    "from_config": {
        "chain": "polygon",
        "data_source": "file",
        "dapp_type": "uniswap",
        "uniswap_config": {
            "pool_address": "0x9b08288c3be4f62bbf8d1c20ac9c5e6f9467d8b7"
        },
        "aave_config": null,
        "big_query": null,
        "chifra_config": null,
        "rpc": null,
        "file": {
            "files": [
                "./sample/polygon-0x45dda9cb7c25131df268515131f647d726f50608-2023-11-25.raw.csv"
            ],
            "folder": null
        },
        "http_proxy": null
    },
    "to_config": {
        "type": "minute",
        "save_path": "./sample",
        "multi_process": false,
        "skip_existed": false,
        "to_file_list": {}
    }
}
2023-11-30 23:13:53 Will generate 1 files


2023-11-30 23:13:53 Download finish
2023-11-30 23:13:53 Start generate 1 files
100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  1.00it/s]
Process finished with exit code 0
```


