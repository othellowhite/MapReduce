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


public class oom01
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

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text>output, Reporter reporter) throws IOException 
    {
      int count =0;                
      String[] attr = key.toString().split(",");      
      List<String> list = new ArrayList<String>();

      while(values.hasNext())               
      {
        list.add((values.next()).toString());
        count++;

      }

      String v=Integer.toString(count);
      for(String s:list)
      { 
        output.collect(new Text(s),new Text(v));
      }

    }   

  }




  public static void main(String[] args) throws IOException
  {
    JobConf conf1 = new JobConf(oom01.class);
    conf1.setJobName("oom01");

    conf1.setOutputKeyClass(Text.class);
    conf1.setOutputValueClass(Text.class);

    conf1.setMapperClass(MapA.class);
    //conf.setCombinerClass(Combine.class);
    conf1.setReducerClass(ReduceA.class);

    conf1.setInputFormat(TextInputFormat.class);
    conf1.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf1, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

    JobClient.runJob(conf1);

    JobConf conf2 = new JobConf(Final.class);
    conf2.setJobName("Final");

    conf2.setOutputKeyClass(Text.class);
    conf2.setOutputValueClass(Text.class);

    conf2.setMapperClass(Final.MapB.class);
    //conf.setCombinerClass(Combine.class);
    conf2.setReducerClass(Final.ReduceB.class);

    conf2.setInputFormat(TextInputFormat.class);
    conf2.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf2, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf2, new Path(args[2]));

    JobClient.runJob(conf2);


  }


}  

class Final
{

  public static class MapB extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
  {

    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException
    {
      String[] r = value.toString().split("\r?\n");
      String[] p1= new String[5];

      for(int i=0;i<r.length;i++)
      {
        p1 = r[i].split("\t");               
        output.collect(new Text(p1[0]),new Text(p1[1]));
      }

    }
  }

  public static class ReduceB extends MapReduceBase implements Reducer<Text, Text, Text, Text>
  {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text>output, Reporter reporter) throws IOException 
    {
      int sum=0;
      while(values.hasNext())
      {
        String s = (values.next()).toString();
        int c=Integer.parseInt(s);
        sum+=c;
      }
      float avf =(float)sum/3;
      String count=Float.toString(avf);
      output.collect(key,new Text(count));
    }   

  }

}
