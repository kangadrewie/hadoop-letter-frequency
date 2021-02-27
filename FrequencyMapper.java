import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    	
    	FloatWritable count = new FloatWritable();
    	String s = value.toString();
    	float c = Float.parseFloat(s.replaceAll("[^0-9.]", ""));
    	
    	count.set(c);
    	
    	context.write(new Text(value), count);
    	
    }
}

