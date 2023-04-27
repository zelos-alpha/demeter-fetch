# README

## 设计目标

* 分钟级别下载
  * 继承demeter
  * 直接下载分钟级别的文件
  * 将raw文件处理成分钟级别的文件
* tick级别下载
  * 拷贝原有代码
  * 支持从bigquery下载
  * 考察一下是否支持rpc下载(因为要额外下载proxy的log)
* 两种下载统一参数
* 使用配置文件
* 提供辅助数据的下载(比如代理合约的log), 
* rpc下载可以依照时间为参数, 而不需要自己查高度
* 抽象log下载, 以便支持其他类型的交易

## 设计思路

step1: 从rpc和bigquery方式下载原始文件
step2: 将原始文件导出成不同的格式


## 使用

```shell
python main.py config.toml

```
