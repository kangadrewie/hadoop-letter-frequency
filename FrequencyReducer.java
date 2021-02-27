import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	long totalCharacters = 0;
	
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {
    	context.write(new Text(key), new FloatWritable(69.0f));
    }
}