mkdir input

# create input files 
cp data1.txt input/
cp data2.txt input/

# create input directory on HDFS
hadoop fs -mkdir -p input

# put input files to HDFS
hdfs dfs -put ./input/* input

# run wordcount 
#hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/sources/hadoop-mapreduce-examples-2.7.2-sources.jar org.apache.hadoop.examples.WordCount input output1
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main MyUseCase.java
jar cf wc_usecase.jar MyUseCase*.class

hadoop jar wc_usecase.jar MyUseCase input output

# print the input files
echo -e "\n input data1.txt:"
hdfs dfs -cat input/data1.txt

echo -e "\n input data2.txt:"
hdfs dfs -cat input/data2.txt


# print the output of wordcount
echo -e "\n Top Rated Movie:"
hdfs dfs -cat output/part-r-00000

