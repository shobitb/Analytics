package edu.nyu.analytics.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FanDistribution {

	public static HashMap<String, String> geo = new HashMap<String, String>();

	static class FanDistributionMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			if (geo.containsKey(split[0])) {
				context.write(new Text(split[1].trim()), new Text(geo.get(split[0].trim())));
			}
		}
	}

	static class FanDistributionReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();

			for (Text value : values) {
				if (!map.containsKey(value.toString())) {
					map.put(value.toString(), 1);
				} else {
					int freq = map.get(value.toString());
					freq++;
					map.put(value.toString(), freq);
				}
			}

			for (String country : map.keySet()) {
				context.write(new Text(key), new Text(country + "\t" + map.get(country)));
			}
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/lastfm-dataset-1K/geo.tsv"));
		String line = "";
		while ((line = reader.readLine()) != null) {
			String[] split = line.split("\t");
			if (split.length == 2) {
				if (!split[1].trim().equals("")) {
					geo.put(split[0], split[1]);
				}
			}
		}

		reader.close();
		Configuration conf = new Configuration();
		Job job = new Job(conf, "first");
		job.setJarByClass(FanDistribution.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FanDistributionMapper.class);
		job.setReducerClass(FanDistributionReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}

}
