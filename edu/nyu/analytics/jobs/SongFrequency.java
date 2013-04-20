package edu.nyu.analytics.jobs;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import java.io.*;

public class SongFrequency {

	static HashSet<String> ignoreSongs = new HashSet<String>();
	static Map<String, String> mapOfSongIDVsTrackID = new HashMap<String, String>();

	static class SongFrequencyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String songId = null;

			if (line.split(",").length >= 2)
				songId = line.split(",")[1];

			if (line.split(",").length >= 2 && !ignoreSongs.contains(songId)) {
				String trackId = "";
				if (mapOfSongIDVsTrackID.containsKey(songId)) {
					trackId = mapOfSongIDVsTrackID.get(songId);
				}
				String frequency = line.split(",")[2];
				context.write(new Text(trackId),
						new IntWritable(Integer.parseInt(frequency)));
			}
		}
	}

	static class SongFrequencyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int totalFreq = 0;
			for (IntWritable value : values) {
				totalFreq = totalFreq + value.get();
			}
			context.write(key, new IntWritable(totalFreq));
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err
					.println("Usage: SongFrequency <input path> <output path>");
			System.exit(-1);
		}

		BufferedReader reader = new BufferedReader(new FileReader(
				"/Users/hiral/Documents/RealTimeBigData/Data/ignore.txt"));
		String line = "";
		while ((line = reader.readLine()) != null) {
			ignoreSongs.add(line);
		}
		reader.close();

		BufferedReader reader1 = new BufferedReader(
				new FileReader(
						"/Users/hiral/Documents/RealTimeBigData/Data/allTrackEchonestId.txt"));
		String line1 = "";
		while ((line1 = reader1.readLine()) != null) {
			String[] arr = line1.split(",");
			mapOfSongIDVsTrackID.put(arr[1], arr[0]);
		}
		reader1.close();

		Job job = new Job();
		job.setJarByClass(SongFrequency.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SongFrequencyMapper.class);
		job.setReducerClass(SongFrequencyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);

	}

}