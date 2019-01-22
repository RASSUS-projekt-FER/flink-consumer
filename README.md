A Flink application project using Scala and SBT.

To run and test your application locally, you can just execute `sbt run` then select the main class that contains the Flink job . 

Package the application into a fat jar with `sbt assembly`, then submit it to Flink for execution 

---

### Dominik - start/stop Flink
Note: read in raw because md formats text  
nc -l 9000  
<run cd ~/Desktop/rassus/flink-1.7.0/ for every remaining terminal window>  
(new terminal) bin/start-cluster.sh  
(new terminal) bin/flink run ~/Dropbox/7.\ semestar/Raspodijeljeni\ sustavi/Lab/flink-consumer/target/scala-2.11/flink-consumer-assembly-0.1-SNAPSHOT.jar  
(new terminal) tail log/flink-*-taskexecutor-*.out -f  
<do your work...>  
(new terminal) bin/stop-cluster.sh  

### Filip - start/stop Flink
(terminal 1) cd flink-1.7.0  
(terminal 1) bin\start-cluster.bat  
(terminal 1) bin\flink run ..\flink-consumer\target\scala-2.11\flink-consumer-assembly-0.1-SNAPSHOT.jar


### Push messages to kafka
#### Control message
(terminal 1) kafka\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic controls-topic  
{"deviceName": "pc-1", "metricName": "cpu", "aggregationType": "P99", "operator": "GT", "threshold": 0.85} 

#### Metric message
(terminal 2) kafka\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic metrics-topic  
{"deviceName": "pc-1", "metricName": "cpu", "value": 0.9}