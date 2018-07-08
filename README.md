# kafka-connect-phoenix

## Phoenix Sink Connector for Kafka-connect
* Only supported message format is JSON with schema, make sure you configure kafka producer properties
* Phoenix table must be pre-created and primary key column with name as "ROWKEY"


Configurations

Below are the properties that need to be passed in the configuration file:

name | data type | required | description
-----|-----------|----------|------------
pqs.url | string | yes | Phoenix Query Server URL [http:\\host:8765]
event.parser.class | string | yes | PhoenixRecordParser
topics | string | yes | list of kafka topics.
hbase.`<topicname>`.rowkey.columns | string | yes | The columns that represent the rowkey of the hbase table `<topicname>`
hbase.`<topicname>`.family | string | yes | Column family of the hbase table `<topicname>`.
