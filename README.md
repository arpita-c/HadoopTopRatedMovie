Use Case : Find the top rated movie per year based on the movie likes and movie rating given the input
dataset.
Input : Input is in the form of votes,rating, movie-name, year



-------------------------------------------------------------------------------
1. The folder contains task5.sh executable file,MyUseCase.java,data1.txt,data2.txt.
2. If it is not executable make it executable by <chmod +x task5.sh>
3. Run task5: 
   ./task5.sh
		
	
---------------------------------------------------------------
Note:: Between each run of task, flush the input output directory
from the HDFS to make a fresh start

Execute the following command before each run:

hdfs dfs  -rm  output/*
hdfs dfs -rmdir output/
hdfs dfs  -rm  input/*
hdfs dfs -rmdir input/
----------------------------------------------------------------
