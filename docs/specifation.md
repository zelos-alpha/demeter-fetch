# Data specifation

The downloaded data is grouped by date, that is, one file per day.

The format of the file name is "[chain name]-[Pool/token contract address]-[date].[file type].csv". e.g.
polygon-0x45dda9cb7c25131df268515131f647d726f50608-2022-01-05.minute.csv

Demeter-fetch will download all files by step. If you set keep_raw=True in config file, In addition to the final result
files, files from intermediate steps will be retained

## raw

Raw files are the original form of on-chain transaction logs, one row for an event log.
Thus, the raw files generated by all sources and all DEFIs have the same format

Samples:

* [Uniswap](sample/ethereum-0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640-2024-01-05.raw.csv)
* [AAVE](sample/polygon-aave_v3-0x7ceb23fd6bc0add59e62ac25578270cff1b9f619-2024-01-06.raw.csv)

**Data definition**

| field             | definition                                                          |
|-------------------|---------------------------------------------------------------------|
| block_number      | log in the block, and data will be sorted by block number ascending |
| block_timestamp   | the timestamp to mine block, will be used to show sorted date       |
| transaction_hash  | record transaction hash and log index to remove duplicated log      |
| transaction_index | transaction index to separate transaction data                      |
| log_index         | index for event log                                                 |
| topics            | topics of event log, stored as hex string array                     |
| data              | data of event log, stored as long hex string                        |

* block_number, block_timestamp, log_index used to sort raw data.
* transaction_hash and log_index to remove duplicated log data.
* topics and data provide core data to analysis.

**Sample**

| block_number | block_timestamp | transaction_hash                                                   | transaction_index | log_index | topics                                                                                                                                                                                                               | data                                                                                                                                                                                                                                                                                                                               |
|--------------|-----------------|--------------------------------------------------------------------|-------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 18937382     | 2024/1/5 0:00   | 0x0bb5e0dc49ae0b5a1949beabadc64522dd5615415a168650a3b592a84ae5ee05 | 33                | 169       | ["0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",  "0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",  "0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad"] | 0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffea5d662000000000000000000000000000000000000000000000000002386f26fc1000000000000000000000000000000000000000051fdf2fd9b2526872a9b723450f2000000000000000000000000000000000000000000000000acd43e0e7cf808c50000000000000000000000000000000000000000000000000000000000030985 |
| 18937382     | 2024/1/5 0:00   | 0x3c78e0a92d12b38375da794749bcf8e3e6ac25de29f4ffdf5a9a21f72e0dafde | 95                | 250       | ["0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",  "0x00000000000000000000000068b3465833fb72a70ecdf485e0e4c7bd8665fc45",  "0x0000000000000000000000004fd39c9e151e50580779bd04b1f7ecc310079fd3"] | 0x000000000000000000000000000000000000000000000000000000006a143deefffffffffffffffffffffffffffffffffffffffffffffffff51fb666300f5b1800000000000000000000000000000000000051fde2e16df48cb7232e401582a8000000000000000000000000000000000000000000000000acd43e0e7cf808c50000000000000000000000000000000000000000000000000000000000030985 |

## 1Min Resample Data

Minute file is used in demeter. In this file, event logs are abstracted to market status at that moment, such as price,
total liquidity, apy etc.
For the convenience of backtesting, data is resampled minutely.

**Samples**

* [Uniswap](sample%2Fethereum-0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640-2024-01-05.minute.csv).
* [AAVE](sample%2Fpolygon-aave_v3-0x7ceb23fd6bc0add59e62ac25578270cff1b9f619-2024-01-06.minute.csv).

### Uniswap

**Data definition**

| field                | definition                            |
|----------------------|---------------------------------------|
| **timestamp**        | minutely timestamp                    |
| **netAmount0**       | token0 net value                      |
| **netAmount1**       | token1 net value                      |
| **closeTick**        | close tick in 1min                    |
| **openTick**         | open tick in 1min                     |
| **lowestTick**       | lowest tick in 1min                   |
| **highestTick**      | highest tick in 1min                  |
| **inAmount0**        | token0 amount exchange value          |
| **inAmount1**        | token1 amount exchange value          |
| **currentLiquidity** | swap data get current total liquidity |

**Sample**

| timestamp           | netAmount0   | netAmount1           | closeTick | openTick | lowestTick | highestTick | inAmount0 | inAmount1            | currentLiquidity    |
|---------------------|--------------|----------------------|-----------|----------|------------|-------------|-----------|----------------------|---------------------|
| 2022-01-05 00:00:00 | -9383445114  | 2477485048495721856  | 193921    | 193919   | 193919     | 193921      | 0         | 2477485048495721856  | 1400049692807883305 |
| 2022-01-05 00:01:00 | -35317761550 | 9329849855269298153  | 193929    | 193921   | 193921     | 193929      | 704261153 | 9515626276489030515  | 1400049692807883305 |
| 2022-01-05 00:02:00 | -55692411213 | 14727627582768166668 | 193942    | 193937   | 193937     | 193942      | 499743350 | 14859678472128538245 | 1395128131590419632 |

### AAVE

minute data is decoded from ReserveDataUpdated event log of AAVE pool. Offical document
is [here](https://docs.aave.com/risk/liquidity-risk/borrow-interest-rate)

**Data definition**

| field                 | definition            |
|-----------------------|-----------------------|
| block_timestamp       | minutely timestamp    |
| liquidity_rate        | liquidity rate        |
| stable_borrow_rate    | stable borrow rate    |
| variable_borrow_rate  | variable borrow rate  |
| liquidity_index       | liquidity index       |
| variable_borrow_index | variable borrow index |

**Sample**

| block_timestamp | liquidity_rate | stable_borrow_rate | variable_borrow_rate | liquidity_index | variable_borrow_index |
|-----------------|----------------|--------------------|----------------------|-----------------|-----------------------|
| 2024/1/6 0:00   | 0.003796       | 0.074471           | 0.019464             | 1.008158        | 1.034414              |
| 2024/1/6 0:01   | 0.003796       | 0.074471           | 0.019464             | 1.008158        | 1.034414              |

### Squeeth

**Data definition**

| field           | definition                                                    |
|-----------------|---------------------------------------------------------------|
| block_timestamp | minutely timestamp                                            | 
| norm_factor     | current normalized factor                                     | 
| WETH            | current weth price in weth/usdc, extracted from uniswap pool  | 
| OSQTH           | current oSQTH price in oSQTH/usdc,extracted from uniswap pool |

Sample

| block_timestamp     | norm_factor         | eth                | osqth              |
|---------------------|---------------------|--------------------|--------------------|
| 2024-01-05 00:00:00 | 0.24838421244630335 | 2272.7319494521357 | 0.0581737742469884 |
| 2024-01-05 00:01:00 | 0.24838421244630335 | 2272.7319494521357 | 0.0581737742469884 |
| 2024-01-05 00:02:00 | 0.24838421244630335 | 2272.7319494521357 | 0.0581737742469884 |

## Tick

In the tick file, each line is an event log. The tick file focuses more on transaction actions. e.g. for uniswap, it
presents all swap actions and liquidate providing actions.

**Samples**

* [Uniswap](sample%2Fethereum-0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640-2024-01-05.tick.csv).
* [AAVE](sample%2Fpolygon-aave_v3-0x7ceb23fd6bc0add59e62ac25578270cff1b9f619-2024-01-06.tick.csv).

### Uniswap

**Data definition**

| field                     | definition                                   |
|---------------------------|----------------------------------------------|
| **block_number**          | block number                                 |
| **block_timestamp**       | timestamp                                    |
| **tx_type**               | operation type, SWAP/MINT/BURN/COLLECT       |
| **transaction_hash**      | transaction hash                             |
| **pool_tx_index**         | tx index for  event log  of pool contract    |
| **pool_log_index**        | log  index for  event log  of pool contract  |
| **proxy_log_index**       | log  index for  event log  of proxy contract |
| **sender**                | topic sender data                            |
| **receipt**               | topic receipt data                           |
| **amount0**               | token0 exchange amount                       |
| **amount1**               | token1 exchange amount                       |
| **total_liquidity**       | calc total liquidity from swap data          |
| **total_liquidity_delta** | mint/burn liquidity exchange abs value.      |
| **sqrtPriceX96**          | sqrtx96 price                                |
| **current_tick**          | swap operation at current tick               |
| **position_id**           | nf token id                                  |
| **tick_lower**            | position lower tick                          |
| **tick_upper**            | position upper tick                          |
| **liquidity**             | curent log liquidity data                    |

**Sample**

| block_number | block_timestamp     | tx_type | transaction_hash                                                   | pool_tx_index | pool_log_index | proxy_log_index | sender                                     | receipt                                    | amount0     | amount1             | total_liquidity     | total_liquidity_delta | sqrtPriceX96                       | current_tick | position_id | tick_lower | tick_upper | liquidity          |
|--------------|---------------------|---------|--------------------------------------------------------------------|---------------|----------------|-----------------|--------------------------------------------|--------------------------------------------|-------------|---------------------|---------------------|-----------------------|------------------------------------|--------------|-------------|------------|------------|--------------------|
| 23353830     | 2022-01-05 00:00:02 | SWAP    | 0xa47dbaeb8275a6a6e6f5cda339c3b982e71415a2947d1cbe4b4d0723c02137f6 | 0             | 3              |                 | 0xe592427a0aece92de3edee1f18e0157c05861564 | 0x8b26320912935111300ddaeec15ea9a182ff6f1a | -2988196954 | 788911381103277696  | 1729804611907121590 | 0                     | 1287023799018574070893171027089208 | 193919.0     |             |            |            |                    |
| 23353928     | 2022-01-05 00:06:24 | BURN    | 0xe6f83a25910ec9945e829e24158e795977f4af6b70abae5a05932e9d7e450734 | 56            | 241            | 242.0           | 0xc36442b4a4522e871399cd717abdd847ab11fe88 |                                            | 0           | 8028675937866099451 | 1370269449887500119 | 0                     | 1289037848681739736056052571218067 | 193951.0     | 13283.0     | 193890.0   | 193920.0   | 329754919099238285 |
| 23353928     | 2022-01-05 00:06:24 | COLLECT | 0xe6f83a25910ec9945e829e24158e795977f4af6b70abae5a05932e9d7e450734 | 56            | 245            | 246.0           | 0xc36442b4a4522e871399cd717abdd847ab11fe88 | 0xb020852796bb04e431e6a2f018805c142fbd4a03 | 869413      | 8031239514820639447 | 1370269449887500119 | 0                     | 1289037848681739736056052571218067 | 193951.0     | 13283.0     | 193890.0   | 193920.0   |                    |
| 23354033     | 2022-01-05 00:09:59 | MINT    | 0x19c7d8a662009b5a14b7a0630f07b86ec8bdd663d34a4f674e3ebe5b33ba244b | 31            | 144            | 146.0           | 0xc36442b4a4522e871399cd717abdd847ab11fe88 |                                            | 16738133293 | 2892535954003525198 | 1455262468155384164 | 60134336564964532     | 1288911018771896468642711393513864 | 193949.0     | 13285.0     | 193890.0   | 194040.0   | 60134336564964532  |

### AAVE

**Data definition**

| field             | definition                                       |
|-------------------|--------------------------------------------------|
| block_number      | block number                                     |
| block_timestamp   | timestamp                                        |
| tx_type           | tx type such as borrow, supply etc.              |
| transaction_hash  | transaction hash of event log                    |
| transaction_index | transaction index                                |
| log_index         | log index of event log                           |
| reserve           | token contract address                           |
| owner             | user address                                     |
| amount            | token amount in wei                              |
| liquidator        | liquidator address, only for liquidation         |
| debt_asset        | liquidation debt asset, only for liquidation     |
| debt_amount       | liquidation debt amount, only for liquidation    |
| atoken            | get atoken in repay, 1 yes, 0 no. only for repay |

**Sample**

| block_number | block_timestamp | tx_type       | transaction_hash                                                   | transaction_index | log_index | reserve                                    | owner                                      | amount   | liquidator | debt_asset | debt_amount | atoken |
|--------------|-----------------|---------------|--------------------------------------------------------------------|-------------------|-----------|--------------------------------------------|--------------------------------------------|----------|------------|------------|-------------|--------|
| 51986202     | 2024/1/6 0:06   | AAVE_SUPPLY   | 0x2e893394725600bc73f2bf977b024862a78f447e571924fb12a14d73c3a769e1 | 55                | 177       | 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619 | 0xa77f88c5031648b7903afaca4e2a431f21f07f0c | 1.86E+18 |            |            |             |        |
| 51986321     | 2024/1/6 0:10   | AAVE_WITHDRAW | 0xeba1a9ee6810e9926c5bd35116f53c1a5fe00a5a9efdfec081222328fb305328 | 97                | 425       | 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619 | 0xe3090207a2de94a856ea10a7e1bd36dd6145712b | 6.66E+16 |            |            |             |        |
| 51986321     | 2024/1/6 0:10   | AAVE_SUPPLY   | 0xeba1a9ee6810e9926c5bd35116f53c1a5fe00a5a9efdfec081222328fb305328 | 97                | 448       | 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619 | 0xa21b1745bd19575b08e4912bbb1c89c67dff859b | 3.31E+14 |            |            |             |        |

## Positions

Uniswap position data is simalar to tick, but re-organized from position dimensions.
Owner information is also added.

**Data definition**

| field            | definition                                                                                                 |
|------------------|------------------------------------------------------------------------------------------------------------|
| position_id      | If a position is added via proxy contract, it's token id. If not, it's owner_address-lower_tick-upper-tick |
| tx_type          | mint/burn/collect                                                                                          |
| owner            | Owner of this position                                                                                     |
| tick_lower       | lower tick of this position                                                                                |
| tick_upper       | upper tick of this position                                                                                |
| liquidity        | liquidity of this action.                                                                                  |
| block_number     | block number                                                                                               |
| block_timestamp  | timestamp                                                                                                  |
| transaction_hash | transaction hash                                                                                           |
| pool_tx_index    | tx index                                                                                                   |
| pool_log_index   | log index of pool event                                                                                    |
| proxy_log_index  | log index of proxy event                                                                                   |
| sender           | sender address                                                                                             |
| receipt          | receipt address                                                                                            |
| amount0          | amount of token0                                                                                           |
| amount1          | amount of token1                                                                                           |
| sqrtPriceX96     | current price                                                                                              |
| current_tick     | current tick                                                                                               |

**Sample**

| position_id | tx_type | owner                                      | tick_lower | tick_upper | liquidity | block_number | block_timestamp | transaction_hash                                                   | pool_tx_index | pool_log_index | proxy_log_index | sender                                     | receipt                                    | amount0  | amount1  | sqrtPriceX96 | current_tick |
|-------------|---------|--------------------------------------------|------------|------------|-----------|--------------|-----------------|--------------------------------------------------------------------|---------------|----------------|-----------------|--------------------------------------------|--------------------------------------------|----------|----------|--------------|--------------|
| 621101      | MINT    | 0xc3a60ac0d824346b40e84d09cf035d9b5c4bc7d3 | 197070     | 200490     | 5.27E+13  | 18937721     | ######          | 0x562237adbb39b66355f7d7a7a654fc965aaca70fb335f9792ff1017198949956 | 108           | 205            | 206             | 0xc36442b4a4522e871399cd717abdd847ab11fe88 | 1.74E+08                                   | 1.04E+17 | 1.66E+33 | 199053       |              |
| 622458      | COLLECT | 0x0af9a3a1eb903a6d9d7e48973e57ed3d123eac34 | 198870     | 199400     | 0         | 18942562     | ######          | 0x8ebe653be2ce7f625813bb006c083f98c02e095a5f31299e0037f8cd8f3ec6ae | 145           | 181            | 182             | 0xc36442b4a4522e871399cd717abdd847ab11fe88 | 0x0af9a3a1eb903a6d9d7e48973e57ed3d123eac34 | 3.85E+09 | 1.76E+18 | 1.68E+33     | 199263       |
| 622458      | MINT    | 0x0af9a3a1eb903a6d9d7e48973e57ed3d123eac34 | 198870     | 199400     | 7.01E+15  | 18942589     | ######          | 0xedc73d90bed797e5e59b67f836478017ebcffb777fdf7f37166ebeaee7b5a19d | 109           | 113            | 114             | 0xc36442b4a4522e871399cd717abdd847ab11fe88 | 2.28E+09                                   | 2.88E+18 | 1.68E+33 | 199261       |              |

## User_lp

User-lp describes actions from the user investment dimension. It includes what positions the user invested in, how much
they invested, and the holding time.

**Data definition**

| field       | definition                         |
|-------------|------------------------------------|
| address     | user address                       |
| position_id | position that user invested        |
| tick_lower  | position range                     |
| tick_upper  | position range                     |
| start_time  | action start time                  |
| end_time    | action end time                    |
| liquidity   | current liquidity in this position |
| start_hash  | start transaction hash             |
| end_hash    | end transation hash                |

**Sample**

| address                                    | position_id | tick_lower | tick_upper | start_time     | end_time       | liquidity         | start_hash                                                         | end_hash                                                           |
|--------------------------------------------|-------------|------------|------------|----------------|----------------|-------------------|--------------------------------------------------------------------|--------------------------------------------------------------------|
| 0xb36d0742725f5d6db92227611f0ff3578afd0e29 | 618587      | 198510     | 199380     | 2024/1/5 15:46 | 2024/1/5 16:16 | 3160689410113665  | 0x20b264abfd39dcf706cad33eb0c2cdf8f11fa41ab1fc449838aa6cb15c1cd487 | 0xdd41020aab72390f327dd3fb97514b7c75a55ccf85d183291ccbe111239858f7 |
| 0xb36d0742725f5d6db92227611f0ff3578afd0e29 | 618587      | 198510     | 199380     | 2024/1/5 16:16 | 2024/1/5 22:03 | 9303302658819284  | 0xdd41020aab72390f327dd3fb97514b7c75a55ccf85d183291ccbe111239858f7 | 0x168a988918369cb0585f689e5fa4a82cd0ed6979561b0443aa84c68ac61b6a2f |
| 0xb36d0742725f5d6db92227611f0ff3578afd0e29 | 618587      | 198510     | 199380     | 2024/1/5 22:03 |                | 42146035944809162 | 0x168a988918369cb0585f689e5fa4a82cd0ed6979561b0443aa84c68ac61b6a2f |                                                                    |

This example shows (0xb36d0742725f5d6db92227611f0ff3578afd0e29) user's action on position 618587. They are compiled from
the following actions.

| Action | liquiditiy        | block number | block timestamp     | transaction hash                                                   |
|--------|-------------------|--------------|---------------------|--------------------------------------------------------------------|
| MINT   | 3160689410113665  | 18942052     | 2024-01-05 15:46:59 | 0x20b264abfd39dcf706cad33eb0c2cdf8f11fa41ab1fc449838aa6cb15c1cd487 |
| MINT   | 6142613248705619  | 18942196     | 2024-01-05 16:16:11 | 0xdd41020aab72390f327dd3fb97514b7c75a55ccf85d183291ccbe111239858f7 |
| MINT   | 32842733285989878 | 18943906     | 2024-01-05 22:03:35 | 0x168a988918369cb0585f689e5fa4a82cd0ed6979561b0443aa84c68ac61b6a2f |


