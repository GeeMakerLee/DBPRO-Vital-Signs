### DBPRO-WS20-21-Tutorial 
#### 1. Stream processing with Apache Flink

- [Apache Flink](https://flink.apache.org/)
- [Apache Application Development, DataStream API](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html)
- [A historical account of Apache Flink ](https://www.dima.tu-berlin.de/fileadmin/fg131/Informationsmaterial/Apache_Flink_Origins_for_Public_Release.pdf)

This project requires: 
- Java 8.0 
- Apache Maven 3.6
- one IDE (Eclipse, IntelliJ)

In this tutorial we process a time series data set in streaming mode. 

- The data used in this example is provided by the [Deutscher Wetterdienst (DWD)](https://www.dwd.de/DE/klimaumwelt/cdc/cdc_node.html)
- More information can be found in [CDC-OpenData area](https://opendata.dwd.de/climate_environment/CDC/Readme_intro_CDC_ftp.pdf) from the DWD
- The data set used here was extracted with the R package [rdwd](https://bookdown.org/brry/rdwd/)

We process the daily climate data set **dataDWD-station-ID-433.csv** (available in resources), it includes temperature measurements:
- taken at **"Berlin-Tempelhof"** (station ID 433), (see other stations in Deutschland in [rdwd Interactive map](https://bookdown.org/brry/rdwd/interactive-map.html)),  
- in the period **2019-05-06 until 2020-11-05** 
- format (header removed in the example):
                
      MESS_DATUM,SDK.Sonnenscheindauer,TMK.Lufttemperatur,TXK.Lufttemperatur_Max,TNK.Lufttemperatur_Min
      2019-05-06,5.967,8.5,13.4,3.5
      2019-05-07,6.467,8.8,13.5,4.3
      2019-05-08,9.1,12.2,18.3,2.8
      2019-05-09,2.783,13.9,18.4,10.4
      2019-05-10,2.533,13.2,16.8,8.5
      ...

**Lufttemperatur, Berlin-Tempelhof**
<center><img src="https://github.com/marcelach1/DBPRO-WS20-21-Tutorial/blob/main/src/main/resources/dwd-temperature-ma7days.png" width="700"></center>
<center>Figure 1. Blue line temperature, Red line: moving average of 7 days sliding 1 day.</center>


#### 2. Instructions for visualization using flink-kafka-websockets-d3.js
The example uses Apache Kafka as an intermediate buffer to vizualise stream data in a web application.
The data flow is described in Figure 2.

<center><img src="https://github.com/marcelach1/DBPRO-WS20-21-Tutorial/blob/main/src/main/resources/visual-arch.png" width="600"></center>
<center>Figure 2. Visualization architecture.</center>


An Apache Flink **DataStream job** produces a data stream that is sink into a Kafka topic *dwddata*

A **Node service** implements a Kafka consumer, in this case from a topic *dwddata*
 
A **Web application** consumes data using a websocket and display the stream using d3.js Line 

The example implemented here is based on:
- d3.js Lines [d3.js Line chart](https://www.d3-graph-gallery.com/line)
- [Live Dashboard Using Apache Kafka and Spring WebSocket](https://dzone.com/articles/live-dashboard-using-apache-kafka-and-spring-webso)
- [Live Streaming Data Using Kafka, Node JS, WebSocket and Chart JS](https://medium.com/@stressed83/live-streaming-data-using-kafka-node-js-websocket-and-chart-js-8750acad549c)

The directory **visual** contains html and javascript files for visualization.
 
**Aftern installing Kafka start it and create the topic (here using kafka_2.11-2.2.0)**

    $ bin/zookeeper-server-start.sh config/zookeeper.properties
    $ bin/kafka-server-start.sh config/server.properties
    $ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic dwddata

**Start node kafka consumer service for the web page**
- **First time, after installing Node.js we need to install some libraries:**

      $ npm install websocket
      $ npm install kafka-node
- **Once installed, run the consumer that is located in the visual directory:**

      $ node kafka_consumer.js dwddata    
    
**Start web page (or open the file in a browser)**

    $ firefox StreamingVisualizationJob.html

**Now produce the data from the Flink job (start the job from IDE)**

After importing the project with and IDE run the StreamingVisualizationJob with the following parameter:

    --input ./src/main/resources/dataDWD-station-ID-433.csv


#### 3. Useful commands in kafka
 
    https://kafka.apache.org/quickstart

    # start zookeper
    bin/zookeeper-server-start.sh config/zookeeper.properties
    # start kafka
    bin/kafka-server-start.sh config/server.properties

    # create a topic	
    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test

    # list topics:
    bin/kafka-topics.sh --list --bootstrap-server localhost:9092

    # delete a topic
    bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test

    # read content of a topic
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

