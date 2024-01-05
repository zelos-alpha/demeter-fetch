# File format

The downloaded data is grouped by date, that is, one file per day. 

The format of the file name is "[chain name]-[Pool/token contract address]-[date].[file type].csv". e.g. polygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv

To prevent download failure, demeter-fetch will download all files first, then convert raw files to minute/tick files. So after the download is completed, the target folder will save two kinds of files, .raw.csv and .minute.csv/.tick.csv.


* Raw file is original transaction event log, one row for an event log, [sample](sample/polygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.raw.csv).
* Minute file is used in demeter. In this file, event logs are abstracted to market data, such as price, total liquidity, apy etc. For the convenience of backtesting, data is resampled minutely. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv). 
* Like minute file, in tick file, event logs are also abstracted to market data, but data will not be resampled. so one row for an event log. Some transaction information such as block number and transaction hash is also kept. It is often used for market analysis. [sample](sample%2Fpolygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.tick.csv)

## Raw Data

### Raw Data definition

| field                | definition                                                                         |
|----------------------|------------------------------------------------------------------------------------|
| **block_number**     | log in the block, and data will be sorted by block number ascending                |
| **block_timestamp**  | the timestamp to mine block, will be used to show sorted date                      |
| **transaction_hash** | record transaction hash and log index to remove duplicated log                     |
| **pool_tx_index**    | transaction index to separate transaction data                                     |
| **pool_log_index**   | log index to remove duplicated data                                                |
| **pool_topics**      | swap/mint/burn/collect/decrease_liquidity/increase_liquidity keccak and other data |
| **pool_data**        | log value in the data field                                                        |
| **proxy_topics**     | nftmanager contract log topics                                                     |
| **proxy_data**       | nftmanager log value in the data field                                             |

* block_number, block_timestamp, log_index used to sort raw data.  
* transaction_hash and log_index to remove duplicated log data.
* topics and data provide core data to analysis.

## Raw sample csv data

| block_number | block_timestamp | transaction_hash | pool_tx_index | pool_log_index | pool_topics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | pool_data | proxy_topics | proxy_data | proxy_log_index |
|---|---|---|---|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|---|---|---|
| 23353830 | 2022-01-05 00:00:02+00:00 | 0xa47dbaeb8275a6a6e6f5cda339c3b982e71415a2947d1cbe4b4d0723c02137f6 | 0 | 3 | "['0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67', '0x000000000000000000000000e592427a0aece92de3edee1f18e0157c05861564', '0x0000000000000000000000008b26320912935111300ddaeec15ea9a182ff6f1a']"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffff4de3bba60000000000000000000000000000000000000000000000000af2c6b8a5ec76800000000000000000000000000000000000003f74862e71484308ef7bb48a1738000000000000000000000000000000000000000000000000180180212f8e71b6000000000000000000000000000000000000000000000000000000000002f57f | [] |  | |
| 23353834 | 2022-01-05 00:00:18+00:00 | 0x7566ec0d48f21e53c84fd267bf87443e094f5cacfe240690cae1a3bf0aaf2529 | 1 | 5 | "['0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67', '0x000000000000000000000000e592427a0aece92de3edee1f18e0157c05861564', '0x000000000000000000000000693fb96fdda3c382fde7f43a622209c3dd028b98']"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffe82d041e0000000000000000000000000000000000000000000000000176f04cdd60157000000000000000000000000000000000000003f75b6ca5fbbd6df2cce1d68e000000000000000000000000000000000000000000000000000136df9c71fb56a29000000000000000000000000000000000000000000000000000000000002f581 | [] |  | |
| 51214980 | 2023-12-17 00:05:11+00:00 | 0x9199209a3aa7c33e74595b381d2c3a46820368d3336731e9160583eede5b1397 | 2 | 9 | "['0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0', '0x000000000000000000000000c36442b4a4522e871399cd717abdd847ab11fe88', '0x0000000000000000000000000000000000000000000000000000000000030a3e', '0x0000000000000000000000000000000000000000000000000000000000030a48']" | 0x00000000000000000000000087d1ed6f4d3865079851d6a02fe5d59f7d4d4ce7000000000000000000000000000000000000000000000000000000012f5ca585000000000000000000000000000000000000000000000000000587ef1f740eaa | "['0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01', '0x000000000000000000000000000000000000000000000000000000000012abfb']" | 0x00000000000000000000000087d1ed6f4d3865079851d6a02fe5d59f7d4d4ce7000000000000000000000000000000000000000000000000000000012f5ca585000000000000000000000000000000000000000000000000000587ef1f740eaa | 10 |

## Tick Data

### Tick Data definition
| field               | definition                           |
|---------------------|--------------------------------------|
| **block_number** | same as raw data                     |
| **block_timestamp**                | same as raw data                     |
| **tx_type**                | operation type, SWAP/MINT/BURN/COLLECT |
| **transaction_hash**                | same as raw data                     |
| **pool_tx_index**                | same as raw data                     |
| **pool_log_index**                | same as raw data                     |
| **proxy_log_index**                | same as raw data                     |
| **sender**                | topic sender data                    |
| **receipt**                | topic receipt data                   |
| **amount0**                | token0 exchange amount               |
| **amount1**                | token1 exchange amount               |
| **total_liquidity**                | calc total liquidity from swap data  |
| **total_liquidity_delta**                | log liquidity change data            |
| **sqrtPriceX96**                | sqrtx96 price                        |
| **current_tick**                | swap operation at current tick       |
| **position_id**                | nf token id                          |
| **tick_lower**                | position lower tick                  |
| **tick_upper**                | position upper tick                  |
| **liquidity**                | curent log liquidity data            |

* total_liquidity: swap liquidity data.
* total_liquidity_delta: mint/burn liquidity exchange abs value.

### tick sample csv data
| block_number | block_timestamp      | tx_type | transaction_hash | pool_tx_index | pool_log_index | proxy_log_index | sender | receipt | amount0 | amount1 | total_liquidity | total_liquidity_delta | sqrtPriceX96 | current_tick | position_id | tick_lower | tick_upper | liquidity |
| --- |----------------------| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 23353830 |  2022-01-05 00:00:02 | SWAP | 0xa47dbaeb8275a6a6e6f5cda339c3b982e71415a2947d1cbe4b4d0723c02137f6 | 0 | 3 |  | 0xe592427a0aece92de3edee1f18e0157c05861564 | 0x8b26320912935111300ddaeec15ea9a182ff6f1a | -2988196954 | 788911381103277696 | 1729804611907121590 | 0 | 1287023799018574070893171027089208 | 193919.0 |  |  |  |  |
| 23353928 | 2022-01-05 00:06:24  | BURN | 0xe6f83a25910ec9945e829e24158e795977f4af6b70abae5a05932e9d7e450734 | 56 | 241 | 242.0 | 0xc36442b4a4522e871399cd717abdd847ab11fe88 |  | 0 | 8028675937866099451 | 1370269449887500119 | 0 | 1289037848681739736056052571218067 | 193951.0 | 13283.0 | 193890.0 | 193920.0 | 329754919099238285 |
| 23353928 | 2022-01-05 00:06:24  | COLLECT | 0xe6f83a25910ec9945e829e24158e795977f4af6b70abae5a05932e9d7e450734 | 56 | 245 | 246.0 | 0xc36442b4a4522e871399cd717abdd847ab11fe88 | 0xb020852796bb04e431e6a2f018805c142fbd4a03 | 869413 | 8031239514820639447 | 1370269449887500119 | 0 | 1289037848681739736056052571218067 | 193951.0 | 13283.0 | 193890.0 | 193920.0 |  |
| 23354033 | 2022-01-05 00:09:59  | MINT | 0x19c7d8a662009b5a14b7a0630f07b86ec8bdd663d34a4f674e3ebe5b33ba244b | 31 | 144 | 146.0 | 0xc36442b4a4522e871399cd717abdd847ab11fe88 |  | 16738133293 | 2892535954003525198 | 1455262468155384164 | 60134336564964532 | 1288911018771896468642711393513864 | 193949.0 | 13285.0 | 193890.0 | 194040.0 | 60134336564964532 |

## 1Min Resample Data

### 1Min Resample Data definition
| field               | definition                            |
|---------------------|---------------------------------------|
| **timestamp** | timestamp use to sort data            |
| **netAmount0**                | token0 net value                      |
| **netAmount1**                | token1 net value                      |
| **closeTick**                | close tick in 1min                    |
| **openTick**                | open tick in 1min                     |
| **lowestTick**                | lowest tick in 1min                   |
| **highestTick**                | highest tick in 1min                  |
| **inAmount0**                | token0 amount exchange value          |
| **inAmount1**                | token1 amount exchange value          |
| **currentLiquidity**                | swap data get current total liquidity |

### 1Min Resample sample csv data
| timestamp | netAmount0 | netAmount1 | closeTick | openTick | lowestTick | highestTick | inAmount0 | inAmount1 | currentLiquidity |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2022-01-05 00:00:00 | -9383445114 | 2477485048495721856 | 193921 | 193919 | 193919 | 193921 | 0 | 2477485048495721856 | 1400049692807883305 |
| 2022-01-05 00:01:00 | -35317761550 | 9329849855269298153 | 193929 | 193921 | 193921 | 193929 | 704261153 | 9515626276489030515 | 1400049692807883305 |
| 2022-01-05 00:02:00 | -55692411213 | 14727627582768166668 | 193942 | 193937 | 193937 | 193942 | 499743350 | 14859678472128538245 | 1395128131590419632 |
