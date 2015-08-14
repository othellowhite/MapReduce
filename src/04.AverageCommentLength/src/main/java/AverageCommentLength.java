import java.io.IOException;
import java.text.SimpleDateFormat;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageCommentLength extends Mapper<Object, Text, Text, CountAverage>{
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
	
	public static class ACLMapper extends Mapper<Object, Text, IntWritable, CountAverage> {
		private IntWritable outTime = new IntWritable();
		private CountAverage outTupleAvg = new CountAverage();
		
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Parse the input string into a nice map
			Map<String, String> parsed = transformXmlToMap(value.toString());
			
			// Grab the "CreationDate" and "Text" field, since that is what we are counting over
			String creationDate = parsed.get("CreationDate");
			String text = parsed.get("UserId");
			
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
				outTupleAvg.setCount(1);
				outTupleAvg.setLength(text.length());
				
				context.write(outTime, outTupleAvg);
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class ACLReducer extends Reducer<IntWritable, CountAverage, IntWritable, CountAverage> {
		private CountAverage result = new CountAverage();
		
		public void reduce(IntWritable key, Iterable<CountAverage> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int cnt = 0;
			result.setCount(0);
			result.setLength(0);
			
			for (CountAverage val : values) {
				sum += val.getLength() * val.getCount();
				cnt += val.getCount();
			}
			
			result.setCount(cnt);
			result.setLength(sum / cnt);
			
			
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AverageCommentLength");

		job.setJarByClass(AverageCommentLength.class);
		job.setMapperClass(ACLMapper.class);
		job.setCombinerClass(ACLReducer.class);
		job.setReducerClass(ACLReducer.class);

		// if mapper outputs are different, call setMapOutputKeyClass and
		// setMapOutputValueClass
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CountAverage.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(CountAverage.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
	}
}
