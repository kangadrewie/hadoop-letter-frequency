import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LanguagePartitioner extends Partitioner<Text, FloatWritable> 
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