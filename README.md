# Interns kafka java client application consist of such commands:
## Create topic

_Required params:_

    --bootstrap-server
    --topic
    --partitions
    --replication-factor

_Optional params:_

    --config-file    

_Example:_
```
create_topic --bootstrap-server=localhost:9092 --topic=topic --partitions=3 --replication-factor=1 --config-file=absolute-path-to-config-file
```

## Delete topic

_Required params:_

    --bootstrap-server
    --topic

_Example:_
```
delete_topic --bootstrap-server=localhost:9092 --topic=topic
```

## Purge topic

_Required params:_

    --bootstrap-server
    --topic
    --group

_Example:_
```
purge_topic --bootstrap-server=localhost:9092 --topic=topic --group=group
```

## Get offset for consumer group

_Required params:_

    --bootstrap-server
    --topic
    --partition
    --group

_Example:_
```
get_offset_consumer_group --bootstrap-server=localhost:9092 --topic=topic --partition=1 --group=group
```

## Set offset for consumer group

_Required params:_

    --bootstrap-server
    --topic
    --partition
    --offset
    --group

_Example:_
```
set_offset_consumer_group --bootstrap-server=localhost:9092 --topic=topic --partition=1 --offset=0 --group=group
```

## Read message in topic by timestamp or offset

_Required params:_

    --bootstrap-server
    --topic
    --partition
    --group

_Optional params (only one must be selected):_

    --offset
    --timestamp

_Example:_
```
read_message --bootstrap-server=localhost:9092 --topic=topic --partition=0 --offset=0 --group=group
read_message --bootstrap-server=localhost:9092 --topic=topic --partition=0 --timestamp=0  --group=group
```

## Get topics

_Required params:_

    --bootstrap-server

_Example:_
```
get_topics --bootstrap-server=localhost:9092
```

## Get consumers

_Required params:_

    --bootstrap-server

_Example:_
```
get_consumers --bootstrap-server=localhost:9092
```

## Write from file to topic

_Required params:_

    --bootstrap-server
    --topic
    --source-file

_Example:_
```
write_from_file --bootstrap-server=localhost:9092 --topic=topic --source-file=absolute-path-to-source-file
```

## Create consumer

_Required params:_

    --bootstrap-server
    --group
    --topic

_Example:_
```
create_consumer --bootstrap-server=localhost:9092 --group=group --topic=topic
```

## Get topics for consumer group

_Required params:_

    --bootstrap-server
    --group

_Example:_
```
get_topics_for_consumer_group --bootstrap-server=localhost:9092 --group=group
```

## Rename kafka connector

_Required params:_

    --bootstrap-server
    --group
    --new-group
    --old-connector
    --new-connector
    --connectors-url

_Example:_
```
rename_kafka_connector --bootstrap-server=localhost:9092 --group=group --new-group=new-group --old-connector=old-connector --new-connector=new-connector --connectors-url=http://localhost:8083
```
## Move topic to another connector

_Required params:_

    --bootstrap-server
    --old-connector
    --new-connector
    --topic
    --connectors-url
_Example:_
```
move_topic_to_connector --bootstrap-server=localhost:9092 --old-connector=datagen-pageviews --new-connector=datagen-users --connectors-url=http://localhost:8083 --topic=pageviews
```
