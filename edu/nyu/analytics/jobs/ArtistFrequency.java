package edu.nyu.analytics.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ArtistFrequency {

	static class ArtistFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split("\t");
			context.write(new Text(split[2]), new IntWritable(Integer.parseInt(split[3].trim())));
		}
	}

	static class ArtistFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int totalFreq = 0;
			for (IntWritable value : values) {
				totalFreq = totalFreq + value.get();
			}
			context.write(key, new IntWritable(totalFreq));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "first");
		job.setJarByClass(ArtistFrequency.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ArtistFrequencyMapper.class);
		job.setReducerClass(ArtistFrequencyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}

}
