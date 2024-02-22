# FAQ

## Which source I should use: 

Demeter-fetch supports three sources. They have their own purposes.

* BigQuery: Require a Google account, It is the fastest way to download data, usually 10 seconds for data in one day. It's not free, but the price is acceptable; You can download a day's worth of data for a few cents.
* RPC: If you register an account in alchemy or quicknode, it would be a free way to get data. It would be better if you have your node. But this way is slow. because demeter-fetch will send a lot of requests to get enough data. It would cost 10 minutes for data in one day.
* Chifra: If you have your node, you can set up a chifra client, export from chifra is straightforward. But run your own node is expansive.

## What should I do if I have poor internet connections

Set keep_raw=True and skip_existed=True, then if download process is interrupted, demeter-fetch can restore former work based on existing raw files. 