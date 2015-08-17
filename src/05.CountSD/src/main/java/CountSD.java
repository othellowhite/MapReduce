import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountSD extends Mapper<Object, Text, Text, IntWritable> {
	public static Map<String, String> transformXmlToMap(String xml) {
		// This helper function parses the stackoverflow into a Map for us.
		
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
	
	public static class SDMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		private IntWritable outTime = new IntWritable();
		private IntWritable outComment = new IntWritable();
		
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Parse the input string into a nice map
			Map<String, String> parsed = transformXmlToMap(value.toString());
			
			// Grab the "CreationDate" and "Text" field, since that is what we are counting over
			String creationDate = parsed.get("CreationDate");
			String text = parsed.get("Text");
			
			// .get will return null if the key is not there
			if (creationDate == null || text == null) { // skip this record
				return;
			}
			
			// Get hour information of date information
			Date date_create;
			try {
				date_create = frmt.parse(creationDate);
				outTime.set(date_create.getHours());
					
				// Get the number of all thing, get comment's length information
				outComment.set(text.length());
				context.write(outTime, outComment);
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class SDReducer extends Reducer<IntWritable, CalculateMedNSD, IntWritable, CalculateMedNSD> {
		private CalculateMedNSD result = new CalculateMedNSD();
		private ArrayList<Long> commentLength = new ArrayList<Long> ();
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			long average = 0;
			long variance = 0;
			result.setStdDev(0);	// 표준 편차   
			result.setMedian(0);	// 중앙값   
			result.setVariance(0);	// 분산   
			commentLength.clear();
			
			// Get value like sum and count about average
			for(IntWritable val : values) {
				commentLength.add((long) val.get());
				sum += val.get();
				count += 1;
			}
			average = sum / count;
			Collections.sort(commentLength);
			
			
			// Get median value of comment's length
			if(count % 2 == 0) { // if the count's number is even
				result.setMedian((commentLength.get(count / 2) + commentLength.get(count / 2 - 1)) / 2);
			}
			else { // else if the count's number is odd
				result.setMedian(commentLength.get(count / 2));
			}
			
		
			// Calculate variance value
			for(Long length : commentLength) { 
				variance += (length - average) * (length - average);
			}
			result.setVariance(variance);
			
			// Calculate standard deviation 
			result.setStdDev((long)Math.sqrt(variance / (count - 1)));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AverageCommentLength");

		job.setJarByClass(CountSD.class);
		job.setMapperClass(SDMapper.class);
		job.setCombinerClass(SDReducer.class);
		job.setReducerClass(SDReducer.class);

		// if mapper outputs are different, call setMapOutputKeyClass and
		// setMapOutputValueClass
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
	}
}
