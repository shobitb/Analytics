package edu.nyu.analytics.jobs;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import java.io.*;

public class UserArtistFrequency {

	static HashSet<String> ignoreSongs = new HashSet<String>();
	static Map<String, String> mapOfSongIDVsArtistID = new HashMap<String, String>();

	static class UserArtistFrequencyMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String user = line.split(",")[0];
			String songId = line.split(",")[1];
			if (!ignoreSongs.contains(songId)) {
				String frequency = line.split(",")[2];
				String artistId = mapOfSongIDVsArtistID.get(songId);
				context.write(new Text(user), new Text(artistId + "," + frequency));
			}
		}
	}

	static class UserArtistFrequencyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Float> tmp = new HashMap<String, Float>();
			String line = "";
			for (Text value : values) {
				String artist = value.toString().split(",")[0];
				String freq = value.toString().split(",")[1];
				if (tmp.containsKey(artist)) {
					tmp.put(artist, tmp.get(artist) + Float.parseFloat(freq));
				} else {
					tmp.put(artist, Float.parseFloat(freq));
				}
			}
			for (Map.Entry<String, Float> entry : tmp.entrySet()) {
				line = entry.getKey() + "\t" + entry.getValue();
				context.write(key, new Text(line));
			}

		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: SongFrequency <input path> <output path>");
			System.exit(-1);
		}

		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/ignore.txt"));
		String line = "";
		while ((line = reader.readLine()) != null) {
			ignoreSongs.add(line);
		}
		reader.close();

		reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/track_song_name.txt"));
		line = "";
		while ((line = reader.readLine()) != null) {
			mapOfSongIDVsArtistID.put(line.split(",")[1], line.split(",")[2]);
		}

		reader.close();

		Job job = new Job();
		job.setJarByClass(SongFrequency.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(UserArtistFrequencyMapper.class);
		job.setReducerClass(UserArtistFrequencyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

	}

}