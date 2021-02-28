import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CharacterSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {	
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {

    	float characterCount = 0f;
    	// Iterate through KV pairs and calculate total count for each character
    	for (FloatWritable value : values) {
    		characterCount += value.get();
    	}
    	
    	// Write total to key
    	context.write(new Text(key), new FloatWritable(characterCount));
    }
}