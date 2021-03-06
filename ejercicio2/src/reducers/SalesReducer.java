package reducers;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;


public class SalesReducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable> {
	public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text,LongWritable> output, Reporter reporter) throws IOException {
		long totalAmount = 0L;

		while (values.hasNext()) {
			totalAmount += values.next().get();

		}

		output.collect(key,new LongWritable(totalAmount));
	}    
}
