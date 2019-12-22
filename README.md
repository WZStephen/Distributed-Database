# Distributed-Database-Operation  


Topics (Operations in PostgreSQL):    
  Rangepartition, Roundrobinpartition  
  RangeQuery, PointQuery  
  ParallelSort, Range_insersion,ParallelJoin  
  EquiJoin  
  FindBusinessBasedOnCity, FindBusinessBasedOnLocation  


For EquiJoin----------------------------------------------------------------  
Description: The code would take two inputs, one would be the HDFS location of the file on whichthe equijoin should be performed and other would be the HDFS location of the file, where the output should be stored.

Steps to Run:

1. Configure the Hadoop hdfs environment(2.7.7) and have java virtual machine installed on Ubunut. https://tecadmin.net/setup-hadoop-on-ubuntu/

2. Copy euiqjoin.java in a project workspace of IDE(eclipse) named equijoin, then right click the equijoin project on Package Explore and select Export.

3. Select "Runnable JAR file" under Java folder, click next and select "Copy required libraries into a sub-folder next to generated JAR" and export it as equijoin.jar

4. Open terminal and type "hdfs dfs –mkdir –p /input" and "hdfs dfs –mkdir –p /output" to create two folder on hdfs to store input and output

5. Upload the sample.txt to input folder on hdfs by "hdfs dfs -put ~/sample.txt /input".

6. Then open terminal and type "hadoop jar ~/equijoin.jar equijoin hdfs://localhost:9000/input/sample.txt hdfs://localhost:9000/output" to run the task, where equijoin is the name of main function in the code and 9000 is the port number on Hadoop WebUI.

7. Once task is completed open localhost:9000 and browse into Utilities-->Browse the file system on Hadoop WebUI, type /output on Browse Directory.

8. You will the the output of the task, and select to download and check the result.

(For multiple times of testing, make sure to delete previous output file by "hadoop fs -rm -f -r hdfs://localhost:9000/output")

Sample.txt:-
R, 2, Don, Larson, Newark, 555-3221
S, 1, 33000, 10000, part1
S, 2, 18000, 2000, part1
S, 2, 20000, 1800, part1
R, 3, Sal, Maglite, Nutley, 555-6905
S, 3, 24000, 5000, part1
S, 4, 22000, 7000, part1
R, 4, Bob, Turley, Passaic, 555-8908

Example Output: -
R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1
R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1
R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1
S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908





