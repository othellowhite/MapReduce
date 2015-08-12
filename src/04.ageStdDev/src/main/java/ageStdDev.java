import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;


public class ageStdDev extends Mapper<Object, Text, Text, IntWritable> {
  
  
  // This helper function parses the stackoverflow into a Map for us.
  public static Map<String, String> transformXmlToMap(String xml) {
    Map<String, String> map = new HashMap<String, String>();
    try {
      // exploit the fact that splitting on double quote
      // tokenizes the data nicely for us
      String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
      for (int i = 0; i < tokens.length - 1; i += 2) {
        String key = tokens[i].trim();
        String val = tokens[i + 1];
        map.put(key.substring(0, key.length() - 1), val);
      }
    } catch (StringIndexOutOfBoundsException e) {
      System.err.println(xml);
    }
    return map;
  }
  
  public static class ageStdDevMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
      
      // Parse the ipt string into a nice map tasks
      Map<String, String> parsed = transformXmlToMap(value.toString()); 
       
      // greb the location & age
      String parsed_location =parsed.get("Location");
      String parsed_age =parsed.get("Age");
      
      if (parsed_age == null || parsed_location == null) { // skip this record
        return;
      }
      
      // Unescape the HTML because the data is escaped.
      parsed_location = StringEscapeUtils.unescapeHtml(parsed_location.toLowerCase());   
      
      // Remove some annoying punctuation
      parsed_location = parsed_location.replaceAll("'", "");           // remove single quotes (e.g., can't) 
      parsed_location = parsed_location.replaceAll("[^a-zA-Z]", " ");  // replace the rest with a space
      
      word.set(parsed_location);
      IntWritable age = new IntWritable(Integer.parseInt(parsed_age));
      
      context.write(word, age);
      
    } 
  }
    
  public static class ageStdDevReducer extends Reducer<Text, IntWritable, Text, FloatWritable>  {
    
    private FloatWritable standardDeviation = new FloatWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
     
      int count = 0;
      int sum = 0;
      int average = 0;
      int variance = 0;
      
      ArrayList<Integer> ageArray = new ArrayList<Integer>();
     
      for (IntWritable val : values) { // sum all of values for same key
        sum = sum + val.get() ;
        count = count + 1;
        
        ageArray.add(val.get());
      }
      if (count >= 10) { // ignores the garbage data
        
        average = sum/count;
        sum = 0;
        
        for (Integer age : ageArray) { // sum all of deviations for get variance
          sum = sum + (int) Math.pow((average - age), 2);
        } 
       
        variance = sum/count;
        standardDeviation.set((float)Math.sqrt(variance));
        
        context.write(key, standardDeviation);
      }
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length != 2){
      System.err.println("Usage: ageStdDev <in> <out>");
      System.exit(2);
    }
    
    Job job = new Job(conf, "ageStdDev");
    job.setJarByClass(ageStdDev.class);
    
    job.setMapperClass(ageStdDevMapper.class);
    job.setReducerClass(ageStdDevReducer.class);

    // output-type of map phase
    job.setMapOutputKeyClass(Text.class);   
    job.setMapOutputValueClass(IntWritable.class);
    
    // output-type of reduce phase
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
  
  

