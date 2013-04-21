package edu.nyu.analytics.jobs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {

	static HTable table_songFrequency;
	static Configuration config = HBaseConfiguration.create();
	static ArrayList<String> arr = new ArrayList<String>();
	static Statement statement;

	/**
	 * @param args
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		table_songFrequency = new HTable(config, "songFrequency");
		statement = dbConnect();
		function();
		for (String s : arr) {
//			String tmp = "TRWIPEU128E078997D";
			Get g = new Get(Bytes.toBytes(s));
			Result rs = table_songFrequency.get(g);

			for (KeyValue kv : rs.raw()) {
				// System.out.println("1 --> " + new String(kv.getRow()) + " ");
				// System.out.println("2 --> " + new String(kv.getFamily()) +
				// ":");
				// System.out.println("3 --> " + new String(kv.getQualifier()) +
				// " ");
				// System.out.println("4 --> " + kv.getTimestamp() + " ");
				System.out.println("main thing --> " + s + " --> " + Bytes.toInt(kv.getValue()));
			}
		}

	}

	private static void function() throws SQLException {
		String artistId = "AR633SY1187B9AC3B9";
		ResultSet rs = statement.executeQuery("SELECT track_id FROM songs WHERE artist_id='" + artistId + "'");
		while (rs.next()) {
			String tmp = rs.getString(1);
			arr.add(tmp);
		}
	}

	public static Statement dbConnect() throws ClassNotFoundException {

		Class.forName("org.sqlite.JDBC");
		Connection connection = null;

		try {
			// create a database connection
			connection = DriverManager
					.getConnection("jdbc:sqlite:/Users/hiral/Documents/RealTimeBigData/Data/track_metadata.db");
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
