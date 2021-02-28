import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;

public class CharacterFrequency {
    public static void main(String[] args) throws Exception {
    	
    	System.err.println(args);

    	// Check input arguments are provided
        if (args.length != 2) {
        	System.err.println("Usage: CharacterFrequency <input path> <output path>");
        	System.exit(-1); 
        }
        
        Configuration conf = new Configuration();

    	System.out.println("In Driver now!");
    
    	// Count occurrences of A-Z
    	Job CalculateCharacterSum = Job.getInstance(conf, "CalculateCharacterSum");
    	CalculateCharacterSum.setJarByClass(CharacterFrequency.class);
    	CalculateCharacterSum.setJobName("CharacterFrequency");
    	CalculateCharacterSum.setNumReduceTasks(3);
    	CalculateCharacterSum.setMapperClass(CharacterMapper.class);
    	CalculateCharacterSum.setCombinerClass(CharacterSumReducer.class);
    	CalculateCharacterSum.setReducerClass(CharacterSumReducer.class);
    	CalculateCharacterSum.setMapOutputKeyClass(Text.class);
    	CalculateCharacterSum.setMapOutputValueClass(FloatWritable.class);
    	CalculateCharacterSum.setPartitionerClass(LanguagePartitioner.class);
    	CalculateCharacterSum.setOutputKeyClass(Text.class);
    	CalculateCharacterSum.setOutputValueClass(FloatWritable.class);
    	
    	FileInputFormat.addInputPath(CalculateCharacterSum, new Path(args[0]));
    	FileOutputFormat.setOutputPath(CalculateCharacterSum, new Path(args[1]));   
    	
    	CalculateCharacterSum.waitForCompletion(true);
    	
    	// Call total counters for all three languages
    	long ENG_TOTAL = CalculateCharacterSum.getCounters().findCounter("CharacterMapper$Character", "ENG").getValue();
    	long FR_TOTAL = CalculateCharacterSum.getCounters().findCounter("CharacterMapper$Character", "FR").getValue();
    	long NL_TOTAL = CalculateCharacterSum.getCounters().findCounter("CharacterMapper$Character", "NL").getValue();
    	
    	// Set totals in configuration
    	conf.setLong("ENG_TOTAL", ENG_TOTAL);
    	conf.setLong("FR_TOTAL", FR_TOTAL);
    	conf.setLong("NL_TOTAL", NL_TOTAL);
    	
    	System.out.println(ENG_TOTAL);
    	System.out.println(FR_TOTAL);
    	System.out.println(NL_TOTAL);
    	System.out.println("THIS IS RESULTS");

    	// Start MP process to calculate frequency of characters
    	Job CalculateCharacterFreq = Job.getInstance(conf, "CalculateCharacterFreq");
    	
    	CalculateCharacterFreq.setJarByClass(CharacterFrequency.class);
    	CalculateCharacterFreq.setJobName("CharacterFrequency");
    	CalculateCharacterFreq.setNumReduceTasks(3);
    	CalculateCharacterFreq.setMapperClass(FrequencyMapper.class);
    	CalculateCharacterFreq.setReducerClass(FrequencyReducer.class);
    	CalculateCharacterFreq.setPartitionerClass(LanguagePartitioner.class);
    	CalculateCharacterFreq.setMapOutputKeyClass(Text.class);
    	CalculateCharacterFreq.setMapOutputValueClass(FloatWritable.class);

    	CalculateCharacterFreq.setOutputKeyClass(Text.class);
    	CalculateCharacterFreq.setOutputValueClass(FloatWritable.class);

    	FileInputFormat.addInputPath(CalculateCharacterFreq, new Path(args[1]));
    	FileOutputFormat.setOutputPath(CalculateCharacterFreq, new Path(args[1]+"/characterfrequency/"));

    	CalculateCharacterFreq.waitForCompletion(true);
    }
}