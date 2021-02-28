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
		
		// Parse previous MP as string
    	String k = value.toString();
    	
		long TOTAL = 0;

		// Get counter total for each language
		if (k.contains("ENG")) {
			TOTAL = conf.getLong("ENG_TOTAL", 0);
		} else if (k.contains("FR")) {
			TOTAL = conf.getLong("FR_TOTAL", 0);
		} else if (k.contains("NL")) {
			TOTAL = conf.getLong("NL_TOTAL", 0);
		} else {
			TOTAL = conf.getLong("ENG_TOTAL", 0);
		}
		
    	FloatWritable count = new FloatWritable();
    	
    	// Extract character occurrences using regex
    	float characterOccurances = Float.parseFloat(k.replaceAll("[^0-9.]", ""));
    	
    	// Calculate average
    	float average = characterOccurances / TOTAL;
    	count.set(average);
    	
    	
    	// Write
    	context.write(new Text(value), count);
    	
    }
}

