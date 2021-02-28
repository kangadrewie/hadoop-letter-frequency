import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {	
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {
    	float averageFrequency = 0f;
    	
    	String k = key.toString().replaceAll("[0-9.\\s+]", "");
    	
    	for (FloatWritable value : values) {
    		averageFrequency += value.get();
    	}
    	
    	context.write(new Text(k), new FloatWritable(averageFrequency));
    }
}