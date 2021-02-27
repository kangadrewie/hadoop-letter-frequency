import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {	
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    	
		Configuration conf = context.getConfiguration();
		long TOTAL = conf.getLong("ENG_TOTAL", 0);
		
    	FloatWritable count = new FloatWritable();
    	String s = value.toString();
    	float characterOccurances = Float.parseFloat(s.replaceAll("[^0-9.]", ""));
    	
    	float average = characterOccurances / TOTAL;
    	count.set(average);
    	
    	context.write(new Text(value), count);
    	
    }
}

