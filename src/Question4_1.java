import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;

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
						if (!(tag.equals("") || tag.equals(" "))) {
							context.write(new Text(Country.getCountryAt(x, y).toString()), new StringAndInt(URLDecoder.decode(tag), 1));// TODO decode
						}
					}
				}
			} catch (Exception e) {
				//e.printStackTrace();
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
		// function to sort hashmap by values
		public static HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) {
			List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(hm.entrySet());
			Collections.sort(list, (i1, i2) -> i1.getValue().compareTo(i2.getValue()));
			HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
			for (Map.Entry<String, Integer> el : list) {
				temp.put(el.getKey(), el.getValue());
			}
			return temp;
		}
		
		StringAndInt toRemove = null;
		StringAndInt toOffer = null;
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {
			// PriorityQueue<StringAndInt> tagsR = new PriorityQueue<>(K);
			
			// for (StringAndInt str : values) {
			// 	if(toRemove!= null){
			// 		tagsR.remove(toRemove);
			// 		toRemove = null;
			// 	}
			// 	if (tagsR.contains(str)) {
			// 		tagsR.forEach(i ->{
			// 			if (str.equals(i)) {
			// 				StringAndInt tmp = new StringAndInt();
			// 				tmp.setTag(str.getTag());
			// 				tmp.setHowMany(str.getHowMany() + i.getHowMany());
			// 				toRemove = new StringAndInt(i);
			// 				toOffer = tmp;
			// 			}
			// 		});
			// 		tagsR.offer(toOffer);
			// 	} else {
			// 		tagsR.offer(new StringAndInt(str));
			// 	}
			// }

			

			// String output ="";
			// int i =0;
			// for (StringAndInt stringAndInt : tagsR) {
			// 	if (i>=K) {
			// 		break;
			// 	}
			// 	output+= stringAndInt.getTag()+",";
			// 	i++;
			// }


			//Użyłem HashMap ponieważ ten sposób działa szybciej i nie powoduje powtarzających się kluczy.
			HashMap<String,Integer> tagsR = new HashMap<>();
			for (StringAndInt str : values) {
				if (tagsR.containsKey(str.getTag())) {
					tagsR.replace(str.getTag(), tagsR.get(str.getTag())+str.getHowMany());
				}
				else{
					tagsR.put(str.getTag(),str.getHowMany());
				}
			}
			
			tagsR = sortByValue(tagsR);
			String output = "";
			int i = 0;
			for (Map.Entry<String, Integer> en :tagsR.entrySet()) {
				if (i >= K) {
					break;
				}
				output += en.getKey() + ",";
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
		// Do uruchomienia z poziomu lokalnego.
		// K = 5;
		// String input = "flick_med";
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

		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}