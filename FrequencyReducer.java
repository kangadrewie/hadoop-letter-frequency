import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {	
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {
    	float characterCount = 0f;
    	
    	for (FloatWritable value : values) {
    		characterCount += value.get();
    	}
    	context.write(new Text(key), new FloatWritable(characterCount));
    }
}