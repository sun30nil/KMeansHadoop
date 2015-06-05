package com.Kmeans;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KMeansReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
	public String finalCent = "";
    public String centroidFileName = "centroid11.csv"; // this should match with your hdfs file location
	
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String eachInst = "";
      String instanc = "";
      int inst_counter = 1;
      double[] meanArr = null; 
      
      while (values.hasNext()) {
    	eachInst = values.next().toString();
        instanc += eachInst+";";
        String[] ints_splitter = eachInst.split(",");
        if (inst_counter == 1){
        	meanArr = new double[ints_splitter.length];
        }
        for (int j = 0; j<ints_splitter.length; j++){  // calcuating the avg of all instances
        	meanArr[j] += Double.parseDouble(ints_splitter[j]);
        }
        inst_counter++;
      }//end of all the instances
      
      String newCentrd = "";
      for (int j = 0 ; j< meanArr.length; j++){ //calculating the mean of the instances
      	meanArr[j] = meanArr[j]/inst_counter;
      	if (j<meanArr.length-1){
      		newCentrd += String.valueOf(meanArr[j])+",";
      	}else{
      		newCentrd += String.valueOf(meanArr[j]);
      	}      	
      }
      //deleting the already existing centroids file
      try{
    	  FileSystem fs = FileSystem.get(new Configuration());
          fs.delete(new Path(centroidFileName), true); // delete file, true for recursive
          
      }catch(Exception e){
    	  System.out.println("File Not Found");
      }
     
      //appending the new centroids to the centroids file
      finalCent += newCentrd+"\n";
      System.out.println(newCentrd);
      //System.out.println(finalCent);
      try{
	          Path pt=new Path(centroidFileName);
	          FileSystem fs = FileSystem.get(new Configuration());
	          BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
	         
	          br.write(finalCent);
	          br.close();
      }catch(Exception e){
          System.out.println("File not found");
      }
      
      output.collect(key, new Text(instanc));
	}
}