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

## 使用

```shell
python main.py config.toml

```
