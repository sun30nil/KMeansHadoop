package com.Kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.Kmeans.KMeansDriver;
import com.Kmeans.KMeansMapper;
import com.Kmeans.KMeansReducer;

public class KMeansDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
    	  int counter = 3;  // no of iterations set to 4 
    	  int res = 0;
          while (counter>=0){
        res = ToolRunner.run( new Configuration(), new KMeansDriver(), args);
        counter--;
          }
          System.exit(res);    
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }
        String outFolder = "out";
        Path pt=new Path(outFolder);  //during iteration deleted the previous out file
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(pt))
            fs.delete(pt, true);
      
        	JobConf conf = new JobConf(KMeansDriver.class);
            Job job = new Job(conf);
            job.setJobConf(conf);
            
            conf.setNumMapTasks(2);
            conf.setNumReduceTasks(2);
            
            conf.setJobName("k-means");
            conf.setNumReduceTasks(2);
      	
      	     conf.setOutputKeyClass(Text.class);
      	     conf.setOutputValueClass(Text.class);
      	    	
      	     conf.setMapperClass(KMeansMapper.class);
      	  	 //conf.setCombinerClass(KMeansReducer.class);
      	  	 conf.setReducerClass(KMeansReducer.class);
      	
      	
      	     conf.setInputFormat(TextInputFormat.class);
             conf.setOutputFormat(TextOutputFormat.class);
     	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
      	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      	
      	     JobClient.runJob(conf);
        
        return 0;
    }
}