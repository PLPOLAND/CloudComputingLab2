import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question4_0 {
	static  int K = 10;
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
			
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] rekord = value.toString().split("\t");
			double x = Double.parseDouble(rekord[10]);
			double y = Double.parseDouble(rekord[11]);
			String tags[] = rekord[8].split(",");
			if (Country.getCountryAt(x,y)!=null) {
				for (String tag : tags) {
					context.write(new Text(Country.getCountryAt(x,y).toString()), new Text(URLDecoder.decode(tag)));
					
				}
			}
			
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String,Integer> map = new HashMap<>();
			for (Text text : values) {
				for(String str: text.toString().split(",")){
					if (map.get(str) == null) {
						map.put(str, 1);
					} else {
						map.replace(str, map.get(str) + 1);
					}
				}
			}	
			PriorityQueue<StringAndInt> tags = new PriorityQueue<>(K);
			for (String str : map.keySet()) {
				tags.offer(new StringAndInt(str, map.get(str)));
			}
			String output ="";
			int i =0;
			for (StringAndInt stringAndInt : tags) {
				if (i>=K) {
					break;
				}
				output+= stringAndInt.getTag()+",";
				i++;
			}
			output = output.substring(0, output.length()-1);
			context.write(key, new Text(output));
		}
	}

	// public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	// 	@Override
	// 	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
	// 			throws IOException, InterruptedException {
	// 		int sum = 0;
	// 		for (IntWritable value : values) {
	// 			sum += value.get();
	// 		}
	// 		context.write(key, new IntWritable(sum));
	// 	}
	// } 

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question4_0");
		job.setJarByClass(Question4_0.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setCombinerClass(MyCombiner.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}