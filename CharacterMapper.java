import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CharacterMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	enum Character{ENG, FR, NL}
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String s = value.toString();
        
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        String prefix = filename.substring(0, filename.indexOf("_") + 1);
        
        String[] characters = s.replaceAll("[^a-zA-Z]+", "").replaceAll("[\\s0-9]+", "").split("(?!^)");
        
    	for (String character : characters) {
    		if (character.length() > 0) {
        		context.write(new Text(prefix+character.toLowerCase()), new FloatWritable(1.0f));
        		if (prefix.contains("ENG")) {
        			context.getCounter(Character.ENG).increment(1);        			
        		} else if (prefix.contains("FR")) {
            	    context.getCounter(Character.FR).increment(1); 
        		} else if (prefix.contains("NL")) {
            	    context.getCounter(Character.NL).increment(1); 
        		}
    		}
    	}
    }
}

