import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountMinMax extends Mapper<Object, Text, Text, CompareMinMax> {
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
	
	public static class CMMMapper extends Mapper<Object, Text, Text, CompareMinMax> {
		
		private Text outUserId = new Text();
		private CompareMinMax outTuple = new CompareMinMax();
		private final static long one = 1;
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Parse the input string into a nice map
			Map<String, String> parsed = transformXmlToMap(value.toString());
			
			// Grab the "CreationDate", "userId" field, since that is what we are counting over
			String creationDate = parsed.get("CreationDate");
			String userId = parsed.get("UserId");
			
			// .get will return null if the key is not there
			if (creationDate == null || userId == null) { // skip this record
				return;
			}
			
			Date date_create;
			
			try {
				date_create = frmt.parse(creationDate);
				outTuple.setMin(date_create);
				outTuple.setMax(date_create);
				outTuple.setCount(one);
				
				outUserId.set(userId);
				context.write(outUserId, outTuple);
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public static class CMMReducer extends Reducer<Text, CompareMinMax, Text, CompareMinMax> {
		private CompareMinMax result = new CompareMinMax();
		
		public void reduce(Text key, Iterable<CompareMinMax> values, Context context) throws IOException, InterruptedException {
			result.setCount(0);
			result.setMin(null);
			result.setMax(null);
			int sum = 0;
			
			for (CompareMinMax val : values) {
				// Compare current min value to stored min value
				if(result.getMin() == null || result.getMin().compareTo(val.getMin()) > 0) {
					result.setMin(val.getMin());
				}
				// Compare current max value to stored max value
				if(result.getMax() == null || result.getMax().compareTo(val.getMax()) < 0) {
					result.setMax(val.getMax());
				}
				sum += val.getCount();
			}
			result.setCount(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CountMinMax");

		job.setJarByClass(CountMinMax.class);
		job.setMapperClass(CMMMapper.class);
		job.setCombinerClass(CMMReducer.class);
		job.setReducerClass(CMMReducer.class);

		// if mapper outputs are different, call setMapOutputKeyClass and
		// setMapOutputValueClass
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CompareMinMax.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CompareMinMax.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}