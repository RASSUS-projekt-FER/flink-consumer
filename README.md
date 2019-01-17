A Flink application project using Scala and SBT.

To run and test your application locally, you can just execute `sbt run` then select the main class that contains the Flink job . 

Package the application into a fat jar with `sbt assembly`, then submit it to Flink for execution 

---

### Dominik - start/stop Flink
nc -l 9000  
(new terminal) ../flink-1.7.0/bin/flink run target/scala-2.11/flink-consumer-assembly-0.1-SNAPSHOT.jar --port 9000  
(new terminal) tail ../flink-1.7.0/log/flink-*-taskexecutor-*.out -f  
(stop cluster) ./bin/stop-cluster.sh  

### Windows - start/stop Flink
(terminal 1) cd flink-1.7.0
(terminal 1) bin\start-cluster.bat
(terminal 1) bin\flink run ..\flink-consumer\target\scala-2.11\flink-consumer-assembly-0.1-SNAPSHOT.jar --port 9000

(terminal 2) ncat -l 9000 

{"name": "cpuUsage", "value": "40", "device": {"id": "id-0", "name": "device-0"}}
