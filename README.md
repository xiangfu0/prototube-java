Prototube
=========

Package prototube implements an API to publish strongly-typed events into Kafka.

Applications can publish events into a specific topic. A topic is always associated with a schema
which defines the schema of the event. Events that do not conform with the schemas are rejected.

Internally prototube encodes the events using the following format:

> \<Magic Number\> \<Header\> \<Event\>

* Magic Number: 0x50, 0x42, 0x54, 0x42
* Header: Protobuf-encoded structure that contains the metadata of the event (e.g, timestamp / uuid)
* Event: Protobuf-encoded event

How to use
==========

Compile and build the project
-----------------------------

Prototube is a Maven project. You can run the following maven command to setup the project.

```
mvn clean install -DskipTests
```

Quickstart
----------

Please follow this [Kafka Quickstart](https://kafka.apache.org/quickstart) link to install and start Kafka locally.

Please see [KafkaExample](./prototube-examples/src/main/java/io/altchain/data/examples/KafkaExample.java) as the example application to produce random messages to local Kafka.


Compile your proto file
--------------------

Maven plugin is used to generate java source code from proto files.

Please see [PROTOTUBE-CORE POM](./prototube-core/pom.xml) to compile `example.proto` into package `io.altchain.data.examples`.

Here is the generated file [Example.java](./prototube-core/target/generated-sources/java/io/altchain/data/examples/Example.java)
