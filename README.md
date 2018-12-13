A Flink application project using Scala and SBT.

To run and test your application locally, you can just execute `sbt run` then select the main class that contains the Flink job . 

You can also package the application into a fat jar with `sbt assembly`, then submit it as usual, with something like: 

```
flink run -c org.example.WordCount /path/to/your/project/my-app/target/scala-2.11/testme-assembly-0.1-SNAPSHOT.jar
```


You can also run your application from within IntelliJ:  select the classpath of the 'mainRunner' module in the run/debug configurations.
Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the "Use classpath of module" dropbox. 

---

### Dominik - start/stop Flink
nc -l 9000  
(new terminal) ../flink-1.7.0/bin/flink run target/scala-2.11/flink-consumer-assembly-0.1-SNAPSHOT.jar --port 9000  
(new terminal) tail ../flink-1.7.0/log/flink-*-taskexecutor-*.out -f  
(stop cluster) ./bin/stop-cluster.sh  

### Filip - start/stop Flink
ncat -l 9000  
(new terminal) flink-1.7.0\bin\flink run flink-consumer\target\scala-2.11\flink-consumer-assembly-0.1-SNAPSHOT.jar --port 9000   
(new terminal) tail ?
(stop cluster) flink-1.7.0\bin\stop-cluster.bat