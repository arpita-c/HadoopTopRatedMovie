import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyUseCase {



	//Map Function
    static class GetMapper extends Mapper<LongWritable, Text, Text, Text> {
   
     
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        
        {
            String line = value.toString();
            String[] tokens = line.split("\t");

            if (Integer.parseInt(tokens[0]) > 10000)
            {
                context.write(new Text(tokens[3]), new Text(tokens[0] + "\t" + tokens[1] + "\t" + tokens[2]));
        	}
        }
    }
    
    
   	//Reduce Function
    static class GetReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
           
            float rate_max = Float.MIN_VALUE;
            int vote_max = Integer.MIN_VALUE;
            String op = "";
            int movie_vote;
            float movie_rate;
			String[] tokens;
         
            for (Text value: values) {
         
                tokens = value.toString().split("\t");    
                movie_rate = Float.parseFloat(tokens[1]);
                movie_vote = Integer.parseInt(tokens[0]);
       
                if (movie_rate > rate_max) 
                {
                    rate_max = movie_rate;
                    vote_max = movie_vote;
                    op = tokens[2];
                }
                else if (movie_rate == rate_max) 
                {
                    if (movie_vote > vote_max) 
                    {
                        rate_max = movie_rate;
                        vote_max = movie_vote;
                        op = tokens[2];
                    }
                }
            }
            context.write(key, new Text(op));
        }
    }
    // Main method
    public static void main(String[] args) throws Exception 
    {
    	 Configuration conf = new Configuration();
   		 Job job = Job.getInstance(conf, "Top rated movie");
    	 job.setJarByClass(MyUseCase.class);
    	 job.setMapperClass(GetMapper.class);
         job.setReducerClass(GetReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);

  
    }
}


