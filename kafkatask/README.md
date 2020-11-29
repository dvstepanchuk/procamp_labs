- to create topic "btcusd-transaction-topic" execute 
`create_topic.sh`
- create nifi flow from template "nifi-publish-kafka-template.xml"
- start flow to publish messages into Kafka
- to build the project run
`mvn clean install`.
"kafka-task-1.0-jar-with-dependencies.jar" will be created in "target" folder
- put "kafka-task-1.0-jar-with-dependencies.jar" on master VM and execute it
`java -jar kafka-task-1.0-jar-with-dependencies.jar`
