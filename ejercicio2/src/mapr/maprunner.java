package mapr;

import mapers.SalesMaper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import reducers.SalesReducer;

import java.io.IOException;

public class maprunner {
	public static void main(String[] args) throws IOException{    
	    JobConf conf = new JobConf(mapr.maprunner.class);    
	    conf.setJobName("Caso 5 Bases 2");
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(LongWritable.class);
	    conf.setMapperClass(SalesMaper.class); 
	    conf.setReducerClass(SalesReducer.class);         
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);    
	    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
	    FileInputFormat.setInputPaths(conf,new Path("/datainput/presupuesto.csv"));
	    FileOutputFormat.setOutputPath(conf,new Path("/dataoutput2"));
	    JobClient.runJob(conf);    // indicamos al yarn
	}    
}
