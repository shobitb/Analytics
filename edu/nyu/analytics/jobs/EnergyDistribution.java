package edu.nyu.analytics.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import ncsa.hdf.object.h5.H5File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.nyu.analytics.jobs.ArtistsPopularity.SongFrequencyMapper;
import edu.nyu.analytics.jobs.ArtistsPopularity.SongFrequencyReducer;

public class EnergyDistribution {

	static Statement statement_energyDist, statement_metadata;
	static H5File h5;

	static class EnergyDistributionMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {

				Class.forName("org.sqlite.JDBC");
				Connection conn = DriverManager
						.getConnection("jdbc:sqlite:/Users/hiral/Documents/RealTimeBigData/Data/artist_energy.db");
				Statement stat = conn.createStatement();
				stat.executeUpdate("drop table if exists metadata;");
				stat.executeUpdate("create table metadata (artistid, trackid, songid);");
				PreparedStatement prep = conn.prepareStatement("insert into metadata values (?, ?, ?);");

				ResultSet rs = statement_metadata.executeQuery("SELECT track_id, song_id, artist_id from songs");

				while (rs.next()) {
					String track_id = rs.getNString(1);
					String song_id = rs.getNString(2);
					String artist_id = rs.getString(3);

					prep.setString(1, artist_id);
					prep.setString(2, track_id);
					prep.setString(3, song_id);
					prep.addBatch();
				}

				conn.setAutoCommit(false);
				prep.executeBatch();
				conn.setAutoCommit(true);

				rs.close();
				conn.close();
			} catch (SQLException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: EnergyDistribution <input path> <output path>");
			System.exit(-1);
		}

		statement_energyDist = dbConnect("/Users/hiral/Documents/RealTimeBigData/Data/artist_energy.db");
		statement_metadata = dbConnect("/Users/hiral/Documents/RealTimeBigData/Data/track_metadata.db");

		Configuration conf = new Configuration();
		Job job = new Job(conf, "first");
		job.setJarByClass(EnergyDistribution.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(EnergyDistributionMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);

		System.out.println("First Job Completed.....Starting Second Job");
		System.out.println("Job completion was successful: " + job.isSuccessful());

		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager
				.getConnection("jdbc:sqlite:/Users/hiral/Documents/RealTimeBigData/Data/artist_energy.db");
		Statement stat = conn.createStatement();
		stat.executeUpdate("drop table if exists energDist;");
		stat.executeUpdate("create table energDist (artistid, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);");
		PreparedStatement prep = conn
				.prepareStatement("insert into metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

		BufferedReader br = new BufferedReader(new FileReader("/Users/hiral/Documents/RealTimeBigData/Data/list.txt"));

		String restPath = "/Users/hiral/Documents/RealTimeBigData/Data/";
		String o = null;
		ResultSet rs = null, rs2 = null;
		while ((o = br.readLine()) != null) {
			String filePath = new StringBuilder(restPath).append(o).toString();

			String[] temp = o.split("/");
			String trackid = temp[temp.length - 1].split(".")[0];

			rs = statement_energyDist.executeQuery("SELECT artistid, songid FROM metadata WHERE trackid='" + trackid
					+ "'");
			String artistid = rs.getNString(1);
			// String songid = rs.getNString(2);

			h5 = new H5File(filePath, H5File.READ);
			double energy = HDF5Getters.get_energy(h5);

			int col = (int) (energy % 10);

			rs2 = statement_energyDist.executeQuery("SELECT" + col + " FROM energyDist WHERE artistid='" + artistid
					+ "'");
			int t = 0;

			if (!rs2.wasNull() && rs2 != null)
				t = rs2.getInt(1);

			t++;
			prep.setString(1, artistid);
			prep.setInt(col, t);
		}

		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);

		rs.close();
		rs2.close();
		conn.close();

		br.close();

	}

	public static Statement dbConnect(String filepath) throws ClassNotFoundException {

		Class.forName("org.sqlite.JDBC");
		Connection connection = null;

		try {
			// create a database connection
			connection = DriverManager.getConnection("jdbc:sqlite:" + filepath);
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30); // set timeout to 30 sec.
			return statement;
		} catch (SQLException e) {
			// if the error message is "out of memory",
			// it probably means no database file is found
			System.err.println(e.getMessage());
		}
		return null;
	}

}
