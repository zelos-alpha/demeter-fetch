# v1.0.0

* rewrite core
* full source support for aave
* add type:
  * userswap user_lp
  * add uniswap position
* breaking change:
  * raw data type for uniswap has all changed

# v0.2.0

* import uniswap data from chifra
* fix bug:
  * if batch_size in rpc section is omitted, an error will raise
  * if tmp file has existed in rpc, the last file may be empty
* breaking change:
  * proxy config has moved to from section

# v0.1.8

* fix bug for aave
* add tick output for aave

# v0.1.7

* add config for base chain

# v0.1.6

* support aave
* add config: 
  * skip_existed in "to" section.
  * dapp_type in "From" section.
  * etherscan_api_key for rpc config. 
* breaking change
  * address of uniswap pool is listed in from.uniswap
  * dapp_type is required