import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedList;
//import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question5_1 {
	
	static  int K = 10;
	static HashMap<String, LinkedList<String>> bestTags = new HashMap<String, LinkedList<String>>();
	
	public static class JobOneMapper extends Mapper<LongWritable, Text, Text, StringAndInt2> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] rekord = value.toString().split("\t");
			try {
				double x = Double.parseDouble(rekord[10]);
				double y = Double.parseDouble(rekord[11]);
				String tags[] = rekord[8].split(",");
				if (Country.getCountryAt(x, y) != null) {
					for (String tag : tags) {
						StringAndInt2 tagAndOne = new StringAndInt2(URLDecoder.decode(tag), 1);
						String country = Country.getCountryAt(x, y).toString();
						//System.out.println("Map1: "+country+"\t"+tagAndOne);
						context.write(new Text(country),tagAndOne);

					}
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndInt2, Text, StringAndInt2>{
		@Override
		protected void reduce(Text key, Iterable<StringAndInt2> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String,Integer> tags = new HashMap<String,Integer>();
			for (StringAndInt2 val : values) { 
				String tag = val.getTag();
				if (tags.containsKey(val.getTag())) {
					tags.replace(val.getTag(),tags.get(val.getTag())+val.getHowMany());
				}
				else{
					tags.put(tag, val.getHowMany());
				}
			}
			for (String str : tags.keySet()) {
				//System.out.println("Combiner: "+key.toString()+" "+str+" "+tags.get(str));
				context.write(key, new StringAndInt2(str,tags.get(str)));
			}
		}
	}
	
	public static class JobOneReducer extends Reducer<Text, StringAndInt2, Text, StringAndInt2>{
		@Override
		protected void reduce(Text key, Iterable<StringAndInt2> values, Context context) throws IOException, InterruptedException {
			for(StringAndInt2 val:values) {
				//System.out.println("Reduce1: "+key.toString()+" "+val.getTag()+" "+val.getHowMany().toString());
				context.write(key,  val);
			}
		}
	}
	
//	public static class JobOneReducer extends Reducer<Text, StringAndInt2, Text, StringAndInt2>{
//		@Override
//		protected void reduce(Text key, Iterable<StringAndInt2> values, Context context)
//				throws IOException, InterruptedException {
//			HashMap<String,Integer> tags = new HashMap<String,Integer>();
//			for (StringAndInt2 val : values) { 
//				String tag = val.getTag();
//				if (tags.containsKey(val.getTag())) {
//					tags.replace(val.getTag(),tags.get(val.getTag())+val.getHowMany());
//				}
//				else{
//					tags.put(tag, val.getHowMany());
//				}
//			}
//			for (String str : tags.keySet()) {
//				System.out.println(key.toString()+" "+str+" "+tags.get(str));
//				context.write(key, new StringAndInt2(str,tags.get(str)));
//			}
//		}
//	}
	 
	
//	public static class JobOneReducer extends Reducer<Text, StringAndInt2, Text, Text>{
//	@Override
//	protected void reduce(Text key, Iterable<StringAndInt2> values, Context context)
//			throws IOException, InterruptedException {
//		HashMap<String,Integer> tags = new HashMap<String,Integer>();
//		for (StringAndInt2 val : values) { 
//			String tag = val.getTag();
//			if (tags.containsKey(val.getTag())) {
//				tags.replace(val.getTag(),tags.get(val.getTag())+val.getHowMany());
//			}
//			else{
//				tags.put(tag, val.getHowMany());
//			}
//		}
//		for (String str : tags.keySet()) {
//			String tmp = str+"\t"+tags.get(str).toString();
//			context.write(key, new Text(tmp));
//		}
//	}
//}	
	 
	public static class JobTwoMapper extends Mapper<Text, StringAndInt2, StringAndInt2, Text> {
		@Override
		public void map(Text key, StringAndInt2 value, Context context) throws IOException, InterruptedException {
			try {
				String tag = value.getTag();
				Integer amount = value.getHowMany();
				StringAndInt2 newKey = new StringAndInt2(key.toString(), amount);
				//System.out.println(newKey.getHowMany().toString()+" "+newKey.getTag()+" "+tag);
				context.write(newKey, new Text(tag));
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class JobTwoReducer extends Reducer<StringAndInt2, Text, Text, Text>{
		@Override
		protected void reduce(StringAndInt2 key, Iterable<Text>values, Context context)
				throws IOException, InterruptedException {
			//HashMap<String, LinkedList<String>> bestTags = new HashMap<String, LinkedList<String>>();
			for(Text val:values) {
//				Integer amount = key.getHowMany();
//				String tagAndAmount = val.toString()+"("+amount.toString()+")";
				if(bestTags.containsKey(key.tag.toString())) {
					if(bestTags.get(key.tag.toString()).size() < K) {
						bestTags.get(key.tag.toString()).add(val.toString());
					} else {
						continue;
					}
				} else {
					LinkedList<String> list = new LinkedList<String>();
					list.add(val.toString());
					bestTags.put(key.tag.toString(), list);
				}
			}
			for(String k:bestTags.keySet()) {
				String tags = "";
//				System.out.print(k+": ");
				while(bestTags.get(k).peekFirst() != null) {
					String tmp = bestTags.get(k).pollFirst(); 
					tags += tmp+", ";
//					System.out.print(tmp+", ");
				}
				//System.out.println();
				if(tags.isEmpty()) {
					continue;
				}
				context.write(new Text(k), new Text(tags));
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		K = Integer.parseInt(otherArgs[2]);

		Job job1 = Job.getInstance(conf, "Question5_1");
		job1.setJarByClass(Question5_1.class);
		
		job1.setMapperClass(JobOneMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(StringAndInt2.class);
		job1.setCombinerClass(MyCombiner.class);

		job1.setReducerClass(JobOneReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(StringAndInt2.class);

		FileInputFormat.addInputPath(job1, new Path(input));
		job1.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path("./output1"));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
//		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
//		Job job2 = Job.getInstance(conf2, "Job2");
		Job job2 = Job.getInstance(conf2, "Question5_1");
//		job2.setJarByClass(Job2.class);
//		job2.setMapperClass(Job2.JobTwoMapper.class);
//		job2.setReducerClass(Job2.JobTwoReducer.class);
		job2.setJarByClass(Question5_1.class);
		job2.setMapperClass(JobTwoMapper.class);
		job2.setMapOutputKeyClass(StringAndInt2.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(JobTwoReducer.class);
		job2.setPartitionerClass(NaturalKeyPartitioner.class);
		job2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job2.setSortComparatorClass(CompositeKeyComparator.class);
//		job2.setSortComparatorClass(SortStringAndInt2.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path("./output1"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);
//		job2.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job2, new Path(output));
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}