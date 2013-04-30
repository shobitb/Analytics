package edu.nyu.analytics.jobs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

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

public class SongFrequency_New {

	static Statement stat;
	static PreparedStatement prep;

	static class SongFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String songId = "";
			String artistId = "";

			if (line.split("\t").length >= 3) {
				songId = line.split("\t")[2];
				artistId = line.split("\t")[1];
			}

			String regex = "[a-zA-Z0-9\\p{Punct}\\s]+";

			if (songId.matches(regex) && artistId.matches(regex) && !songId.matches(".*[uU]ntitled.*"))
				context.write(new Text(songId + "\t" + artistId), new IntWritable(1));
		}
	}

	static class SongFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			try {

				int totalFreq = 0;
				for (IntWritable value : values) {
					totalFreq = totalFreq + value.get();
				}

				String[] split = key.toString().split("\t");
				prep.setString(1, split[0]);
				prep.setString(2, split[1]);
				prep.setInt(3, totalFreq);
				prep.addBatch();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			SQLException {
		if (args.length != 3) {
			System.err.println("Usage: SongFrequency <input path> <temp path> <output path>");
			System.exit(-1);
		}

		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager
				.getConnection("jdbc:sqlite:/Users/hiral/Documents/RealTimeBigData/Data/song_frequency.db");
		stat = conn.createStatement();

		stat.executeUpdate("drop table if exists song_frequency;");

		stat.executeUpdate("CREATE TABLE song_frequency ( \"song_name\" TEXT NOT NULL, \"artist_name\" TEXT, \"frequency\" INT );");
		prep = conn.prepareStatement("insert into song_frequency values (?, ?, ?);");

		Configuration conf = new Configuration();
		Job job = new Job(conf, "first");
		job.setJarByClass(SongFrequency.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SongFrequencyMapper.class);
		job.setReducerClass(SongFrequencyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);

		System.out.println("First Job Completed");
		System.out.println("Job completion was successful: " + job.isSuccessful());
		
		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);

		conn.close();

	}

}
