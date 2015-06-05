package com.Kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;


public class KMeansMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public String centroidFileName = "centroid11.csv"; // this should match with your hdfs file location
	
	private Text closing = new Text();
    private Text mykey = new Text();
    public ArrayList<String> al;
    
	public void setup1() throws IOException, InterruptedException {
		
		al = new ArrayList<String>();
		try{
            Path pt=new Path(centroidFileName);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                    al.add(line);
                    line=br.readLine();
            }
    }catch(Exception e){
    }
    }
	

	     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	      try {
			setup1();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      String line = value.toString();
	      double[] ci = new double[al.size()]; 
	      String[] str_inst = line.toString().split(",");
	      for (int i = 0; i< al.size(); i++){
	    	  String[] each_centroid = al.get(i).split(",");
	    	  double meanDist = 0;
	    	  for (int j = 0; j<str_inst.length; j++){
	    		  double feature = Double.parseDouble(str_inst[j]);
	    		  double centroid = Double.parseDouble(each_centroid[j]);
	    		  meanDist += Math.pow((feature - centroid), 2) ;
	    		  
	    	  }
	    	  Double dist = Math.sqrt(meanDist);
	    	  ci[i] = dist;
	      }
	      double minimum = ci[0];
	      String ici = "1";     // class label initialized to class 1
	      for (int i = 1; i<ci.length; i++){
	    	  if(ci[i]<minimum){
	    		  ici = String.valueOf(i+1);  //class falling near to the centroid
	    	  }
	      }
	      mykey.set(ici); //cluster index
	      closing.set(line);  //all instances belonging to that instance
	      System.out.println("key->"+ici+" Val->"+line);
	         output.collect(mykey, closing);
	     }
	   }