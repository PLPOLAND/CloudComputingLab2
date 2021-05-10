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

public class Question4_1 {
	static  int K = 10;
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
			
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] rekord = value.toString().split("\t");
			try {
				double x = Double.parseDouble(rekord[10]);
				double y = Double.parseDouble(rekord[11]);
				String tags[] = rekord[8].split(",");
				if (Country.getCountryAt(x, y) != null) {
					for (String tag : tags) {
						context.write(new Text(Country.getCountryAt(x, y).toString()),
								new StringAndInt(URLDecoder.decode(tag), 1));// TODO decode

					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt>{
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String,Integer> tags = new HashMap<String,Integer>();
			for (StringAndInt val : values) { 
				String tag = val.getTag();
				if (tags.containsKey(val.getTag())) {
					tags.replace(val.getTag(),tags.get(val.getTag())+val.getHowMany());
				}
				else{
					tags.put(tag, val.getHowMany());
				}
			}
			for (String str : tags.keySet()) {
				context.write(key, new StringAndInt(str,tags.get(str)));
			}
		}
	} 

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {
			PriorityQueue<StringAndInt> tagsR = new PriorityQueue<>(K);
			for (StringAndInt str : values) {
				tagsR.offer(new StringAndInt(str));
			}

			String output ="";
			int i =0;
			for (StringAndInt stringAndInt : tagsR) {
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


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		K = Integer.parseInt(otherArgs[2]);
		// String input = "flickrSample.txt";
		// String output = "output";

		Job job = Job.getInstance(conf, "Question4_1");
		job.setJarByClass(Question4_1.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);
		job.setCombinerClass(MyCombiner.class);

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