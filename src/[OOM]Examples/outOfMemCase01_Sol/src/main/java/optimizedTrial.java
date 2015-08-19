// reference: Getting java heap space error while running a mapreduce code for large dataset
// http://stackoverflow.com/questions/23042829/getting-java-heap-space-error-while-running-a-mapreduce-code-for-large-dataset


import java.io.IOException;
import java.util.*;
import java.lang.Iterable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//Phase #1
public class optimizedTrial
{

  public static class MapA extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
  {

    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException
    {
      String[] rows = value.toString().split("\r?\n");          
      for(int i=0;i<rows.length;i++)
      {
        String[] cols = rows[i].toString().split(",");

        String v=cols[0];
        for(int j=1;j<cols.length;j++)
        {
          String k =j+","+cols[j];
          output.collect(new Text(k),new Text(v));
        }
      }
    }
  }


  public static class ReduceA extends MapReduceBase implements Reducer<Text, Text, Text, Text>
  {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text>output, Reporter reporter) 
        throws IOException {
      
      int count = 0;

      while (values.hasNext()) {
          output.collect(key, values.next());
          count++;
      }
      output.collect(new Text("." + key), new Text(Integer.toString(count)));
    }   

  }

  /////////////////////////////////////
  /////////// main Function ///////////
  /////////////////////////////////////
  public static void main(String[] args) throws IOException
  {
    
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length != 4){
      System.err.println("Usage: optimizedTrial <in> <intrm_1> <intrm_2> <out>");
      System.exit(2);
    }
    
    JobConf conf1 = new JobConf(optimizedTrial.class);
    conf1.setJobName("optimizedTrial");

    conf1.setOutputKeyClass(Text.class);
    conf1.setOutputValueClass(Text.class);

    conf1.setMapperClass(MapA.class);
    //conf.setCombinerClass(Combine.class);
    conf1.setReducerClass(ReduceA.class);

    conf1.setInputFormat(TextInputFormat.class);
    conf1.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(conf1, new Path(otherArgs[1]));

    System.out.println("##### phase 1 run #####"); // yoon // chk phase #1
    
    JobClient.runJob(conf1);

    
    JobConf conf2 = new JobConf(Intermediate.class);
    conf2.setJobName("Intermediate");
    
    conf2.setMapperClass(Intermediate.MapB.class);
    conf2.setReducerClass(Intermediate.ReduceB.class);

    conf2.setInputFormat(KeyValueTextInputFormat.class);
    
    // output-type of map phase
    conf2.setMapOutputKeyClass(Text.class);
    conf2.setMapOutputValueClass(Text.class);

    // output-type of reduce phase
    conf2.setOutputKeyClass(Text.class);
    conf2.setOutputValueClass(Text.class);
    
    FileInputFormat.setInputPaths(conf2, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(conf2, new Path(otherArgs[2]));

    System.out.println("##### phase 2 run #####"); // yoon // chk phase #2
    
    JobClient.runJob(conf2);
    
    
    JobConf conf3 = new JobConf(Final.class);
    conf3.setJobName("Final");

    conf3.setMapperClass(Final.MapC.class);
    conf3.setReducerClass(Final.ReduceC.class);

    conf3.setInputFormat(KeyValueTextInputFormat.class);

    // output-type of map phase
    conf3.setMapOutputKeyClass(Text.class);
    conf3.setMapOutputValueClass(Text.class);

    // output-type of reduce phase
    conf3.setOutputKeyClass(Text.class);
    conf3.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(conf3, new Path(otherArgs[2]));
    FileOutputFormat.setOutputPath(conf3, new Path(otherArgs[3]));

    System.out.println("##### phase 3 run #####"); // yoon // chk phase #3
    
    JobClient.runJob(conf3);
    

  }


}  

// Phase #2
class Intermediate
{
  public static class MapB extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
  {
    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
        output.collect(key, value);
    }
  }
  

  public static class ReduceB extends MapReduceBase implements Reducer<Text, Text, Text, Text>
  {
    
    private Map<String, Integer> total_count = new HashMap<String, Integer>();
    private Set<String> attributes = new HashSet<String>(); // count the distinct number of attributes

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text>output, Reporter reporter) 
        throws IOException {
      
      String rKey = key.toString();
      if(rKey.startsWith(".")){
          while (values.hasNext()) {
              total_count.put(rKey.substring(1), Integer.parseInt(values.next().toString()));  
              attributes.add(rKey.substring(1).split(",")[0]);
              return;
          }
      }
      while (values.hasNext()) {
          Text value = values.next();
          output.collect(value, new Text(Integer.toString(total_count.get(rKey))));
          output.collect(value, new Text("." + attributes.size())); // send the total number of attributes
      } 
    }
  }
}

//Phase #3
class Final
{

  public static class MapC extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
  {

    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
        throws IOException {
            output.collect(key, value);
    }
  }

  public static class ReduceC extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable>
  {
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable>output, Reporter reporter) 
        throws IOException {
       long sum = 0;
       int nbAttributes = 0;
       while(values.hasNext()){
           String value = values.next().toString();
           if(value.startsWith(".")){ // check if line corresponds to the total number of attributes
               nbAttributes = Integer.parseInt(value.substring(1)); 
           } 
           else{
               sum += Integer.parseInt(value);   
           }
       }
       output.collect(key, new DoubleWritable(sum / nbAttributes));  
  
    }
  }

}
