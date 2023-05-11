# README

## 设计目标

* (√)分钟级别下载
    * 继承demeter
    * 直接下载分钟级别的文件
    * 将raw文件处理成分钟级别的文件
* (√)tick级别下载
    * 拷贝原有代码
    * 支持从bigquery下载
    * 支持rpc下载(考虑如何兼容proxy合约的log)
* (√)两种下载统一参数
* (√)使用配置文件
* (√)提供辅助数据的下载(比如代理合约的log),
* (√)抽象log下载, 以便支持其他类型的交易

额外功能

* (√)rpc下载可以依照时间为参数, 而不需要自己查高度
* rpc下载支持设置超时时间. 失败后重试
* (√)高度下载支持并行处理

## 设计思路

step1: 从rpc和bigquery方式下载原始文件
step2: 将原始文件导出成不同的格式

规则

* 下载文件名的规则:
    * 原始文件: chain-pool地址-date.raw.csv
    * 处理过的文件: chain-pool地址-date.processed.csv
    * rpc下载过程的临时文件: chain-合约地址-开始高度-结束高度.tmp.pkl

## usage

```shell
python main.py config-sample.toml

```
