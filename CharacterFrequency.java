import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class CharacterFrequency {
    public static void main(String[] args) throws Exception {
    	
    	System.err.println(args);

        if (args.length != 2) {
        	System.err.println("Usage: CharacterFrequency <input path> <output path>");
        	System.exit(-1); 
        }
        
        Configuration conf = new Configuration();

    	System.out.println("In Driver now!");
    
    	Job GetAllCharacters = Job.getInstance(conf, "GetAllCharacters");
    	GetAllCharacters.setJarByClass(CharacterFrequency.class);
    	GetAllCharacters.setJobName("CharacterFrequency");
    	GetAllCharacters.setNumReduceTasks(3);
    	GetAllCharacters.setMapperClass(CharacterMapper.class);
    	GetAllCharacters.setCombinerClass(CharacterSumReducer.class);
    	GetAllCharacters.setReducerClass(CharacterSumReducer.class);
    	GetAllCharacters.setMapOutputKeyClass(Text.class);
    	GetAllCharacters.setMapOutputValueClass(FloatWritable.class);
    	GetAllCharacters.setPartitionerClass(LanguagePartitioner.class);
    	GetAllCharacters.setOutputKeyClass(Text.class);
    	GetAllCharacters.setOutputValueClass(FloatWritable.class);
    	
    	FileInputFormat.addInputPath(GetAllCharacters, new Path(args[0]));
    	FileOutputFormat.setOutputPath(GetAllCharacters, new Path(args[1]));   
    	
    	GetAllCharacters.waitForCompletion(true);
    	
    	Job CalculateSum = Job.getInstance(conf, "CalculateSum");
    	CalculateSum.setJarByClass(CharacterFrequency.class);
    	CalculateSum.setJobName("CharacterFrequency");
    	CalculateSum.setNumReduceTasks(3);
    	CalculateSum.setMapperClass(FrequencyMapper.class);
    	CalculateSum.setReducerClass(FrequencyReducer.class);
    	CalculateSum.setPartitionerClass(LanguagePartitioner.class);
    	CalculateSum.setMapOutputKeyClass(Text.class);
    	CalculateSum.setMapOutputValueClass(FloatWritable.class);

    	CalculateSum.setOutputKeyClass(Text.class);
    	CalculateSum.setOutputValueClass(FloatWritable.class);

    	FileInputFormat.addInputPath(CalculateSum, new Path("output/"));
    	FileOutputFormat.setOutputPath(CalculateSum, new Path("/user/root/output/secondMap/"));

        CalculateSum.waitForCompletion(true);
    }
    
    public static class LanguagePartitioner extends Partitioner<Text, FloatWritable> 
    {
       @Override
       public int getPartition(Text key, FloatWritable values, int numPartitions)
       {
     	  String k = key.toString().toLowerCase();
     	  
     	  if (k.contains("eng")) {
     		  return 0;
     	  } else if (k.contains("fr")) {
     		  return 1;
     	  } else if (k.contains("nl")) {
     		  return 2;
     	  } else {
     		  return 0;
     	  }
       }
    }
    
}