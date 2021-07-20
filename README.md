# moonspark
### 선행진행
+ 제약사항 : MacOS, jdk8+
+ spark
    + spark download(brew or gz)
    + bash_profile 등록 
        + export SPARK_HOME=/Users/lion00/spark-2.1.0-bin-hadoop2.7
        export HADOOP_HOME=/Users/lion00/spark-2.1.0-bin-hadoop2.7
        export PATH=$PATH:$SPARK_HOME/bin
+ zookeeper & kafka
    + download & extract
    + start
        + zkServer.sh start
        + bin/kafka-server-start.sh config/server.properties
    + topic create
        + ./kafka-topics.sh --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test_topic1 --create
    
### 실행방법
+ spark streaming 
    + src/main/resources/in_data 에 csv파일저장 
    + maven package -> jar 생성
    + sparksubmit.sh실행 ( 로컬 standalon spark사용) eg. ./sparksubmit.sh (listings|reviews|or null)



+ [검증] kafka topic consumer 확인
    + ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${topic_name} --from-beginning
    + eg. ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tp-reviews --from-beginning

---
#### version : spark2.1, scala2.11 

---
#### reference
+ https://spark.apache.org/docs/2.1.1/structured-streaming-programming-guide.html

