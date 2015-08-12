import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class distributedGrep extends Mapper<Object, Text, NullWritable, Text>{
  
  public static class dGrepMapper extends Mapper<Object, Text, NullWritable, Text> {
    
    private String mapRegex = null;
    
    public void setup(Context context) {
      mapRegex = context.getConfiguration().get("mapregex");
    }
    
    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
  
      if (value.toString().matches(mapRegex)) {
        context.write(NullWritable.get(), value);
      }
      
    }
  }
  
  public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length != 3){
      System.err.println("Usage: distributedGrep <in> <out> <regEx>");
      System.exit(2);
    }

    conf.set("mapregex", otherArgs[2]);
    
    Job job = new Job(conf, "distributedGrep");
    job.setJarByClass(distributedGrep.class);
    
    job.setMapperClass(dGrepMapper.class);

    // output-type for result
    job.setOutputKeyClass(NullWritable.class);   
    job.setOutputValueClass(Text.class);
    
    job.setNumReduceTasks(0); // reduce worker not needed.
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
