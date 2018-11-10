# kafka-connect-phoenix

## Phoenix Sink Connector for Kafka-connect
* Only supported message format is JSON with schema, make sure you configure kafka producer with appropriate properties
* This derives table columns based on the schema of the message received
* Batch 100 records at a time for a given poll cycle


Configurations

Below are the properties that need to be passed in the configuration file:

name | data type | required | description
-----|-----------|----------|------------
pqs.url | string | yes | Phoenix Query Server URL [http:\\host:8765]
event.parser.class | string | yes | PhoenixRecordParser
topics | string | yes | list of kafka topics.
hbase.`<topicname>`.table.name | string | yes | Phoenix table name in which records needs to be saved for '<topicname>'
