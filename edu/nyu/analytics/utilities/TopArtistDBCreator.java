package edu.nyu.analytics.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.PriorityQueue;

public class TopArtistDBCreator {

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/mbox/output/part-r-00000"));
		String line = "";
		Statement stat;
		PreparedStatement prep;
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:/home/shobit/development/big-data-project/subsets/mbox/top_artists.db");
		stat = conn.createStatement();
		stat.executeUpdate("drop table if exists charts;");
		stat.executeUpdate("CREATE TABLE charts (\"artist_name\" TEXT NOT NULL, \"hits\" TEXT NOT NULL);");
		prep = conn.prepareStatement("insert into charts values (?, ?);");

		while ((line = reader.readLine()) != null) {
			String artist = line.split("\t")[0];
			Integer number = Integer.parseInt(line.split("\t")[1]);
			prep.setString(1, artist);
			prep.setString(2, number + "");
			prep.addBatch();
		}
		reader.close();

		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);

		conn.close();
	}
}
