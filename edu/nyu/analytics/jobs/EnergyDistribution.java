package edu.nyu.analytics.jobs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import ncsa.hdf.object.h5.H5File;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EnergyDistribution {

	static Statement statement_energyDist, statement_metadata;
	static H5File h5;

	static class EnergyDistributionMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				ResultSet rs = statement_metadata.executeQuery("SELECT track_id, artist_id from songs");

				while (rs.next()) {
					String track_id = rs.getNString(1);
					String artist_id = rs.getString(2);

				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: EnergyDistribution <input path> <output path>");
			System.exit(-1);
		}

		h5 = new H5File("/Users/hiral/Documents/RealTimeBigData/Data/MillionSongSubset", H5File.READ);
		
		statement_energyDist = dbConnect("/Users/hiral/Documents/RealTimeBigData/Data/artist_energy.db");
		statement_metadata = dbConnect("/Users/hiral/Documents/RealTimeBigData/Data/track_metadata.db");

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
