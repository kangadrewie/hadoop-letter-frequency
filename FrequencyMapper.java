import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {	
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    	String k = key.toString();
    	context.write(new Text(k), new FloatWritable(69.0f));
    }
}

