import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountLocation extends Mapper<Object, Text, Text, IntWritable>
{
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
	
	public static class CLMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Parse the input string into a nice map
			Map<String, String> parsed = transformXmlToMap(value.toString());
			
			// Grab the "Location" field, since that is what we are counting over
			String txt = parsed.get("Location");
			
			// .get will return null if the key is not there
			if (txt == null) { // skip this record
				return;
			}
			
			// Unescape the HTML because the data is escaped.
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
			
			// Remove some annoying punctuation
			txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
			txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a space
			
			word.set(txt);
			context.write(word, one);
			
		}
	}
	
	public static class CLReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CountLocation");

		job.setJarByClass(CountLocation.class);
		job.setMapperClass(CLMapper.class);
		job.setCombinerClass(CLReducer.class);
		job.setReducerClass(CLReducer.class);

		// if mapper outputs are different, call setMapOutputKeyClass and
		// setMapOutputValueClass
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*// An InputFormat for plain text files. Files are broken into lines.
		// Either linefeed or carriage-return are used to signal end of line.
		// Keys are the position in the file, and values are the line of text..
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);*/

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
    
}
