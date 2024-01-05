## 3 Benchmark

|               | one day data cost(seconds) | recommended | reason                                          |
|---------------|----------------------------|-------------|-------------------------------------------------|
| rpc           | 575s                       | ☆☆          | cost too much time to get data from rpc service |
| bigquery      | 9s                         | ☆☆☆☆☆       | very fast and only cost about 0.05 usd/day data |
| file          | 1s                         | ☆☆☆         | just convert downloaded raw data                |
| chifra local  | 6s                         | ☆☆          | just merge local downloaded eth data            |
| chifra export | 61s                        | ☆☆☆         | export chifra data and only support ethereum    |

