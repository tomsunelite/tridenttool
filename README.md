Welcome to TridentTool!
===================
A simple tool used to check the storm trident meta data in zookeeper for transactions, and fetch the messages from kafka to find the root case when some emitter can not process the batch correctly.

----------

Usage
-------------

1. get the trident transactions
`
java -jar TidentTool-1.0-jar-with-dependencies.jar listTx -z=10.4.5.107:2181,10.4.5.108:2181 -r /transactional
java -jar TidentTool-1.0-jar-with-dependencies.jar listTx
Missing required options: z, r
usage: TridentTool
 -r,--rootpath <arg>    the kafka/transaction root path, /kafka or
                        /transactional
 -z,--zookeeper <arg>   zookeeper connect string,xxx.xxx.xxx.xxx:2181
`

2. get the meta data for specified transaction id
`
java -jar TidentTool-1.0-jar-with-dependencies.jar getTxMeta -z=10.4.5.107:2181 -r=/transactional -i=hqTxId -p 0
java -jar TidentTool-1.0-jar-with-dependencies.jar getTxMeta -z=10.4.5.107:2181 -r=/transactional 
Missing required options: i, p
usage: TridentTool
 -i,--txid <arg>        the transaction id string
 -p,--partition <arg>   the partition number
 -r,--rootpath <arg>    the kafka/transaction root path, /kafka or
                        /transactional
 -z,--zookeeper <arg>   zookeeper connect string,xxx.xxx.xxx.xxx:2181
`
3.  get the kafka message from specified offset
`
java -jar TidentTool-1.0-jar-with-dependencies.jar fetchMessage -z=10.4.5.107:2181 -r=/kafka -t=scraper.posts -p=0 -o=55800765 -b=10240
java -jar TidentTool-1.0-jar-with-dependencies.jar fetchMessage -z=10.4.5.107:2181 -r=/kafka
Missing required options: t, p, o, b
usage: TridentTool
 -b,--bytes <arg>       the bytes need to fetch
 -o,--offset <arg>      the offset start from
 -p,--partition <arg>   the partition number
 -r,--rootpath <arg>    the kafka/transaction root path, /kafka or
                        /transactional
 -t,--topic <arg>       the kafka topic name
`
