package edu.nyu.analytics.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DemographicDBCreator {

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/lastfm-dataset-1K/demo/output/part-r-00000"));
		String line = "";
		Statement stat;
		PreparedStatement prep;
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:/home/shobit/development/big-data-project/subsets/lastfm-dataset-1K/demo/output/demo.db");
		stat = conn.createStatement();
		stat.executeUpdate("drop table if exists demo;");
		stat.executeUpdate("CREATE TABLE demo (\"artist_name\" TEXT NOT NULL, \"gender\" TEXT NOT NULL, \"value\" TEXT NOT NULL);");
		prep = conn.prepareStatement("insert into demo values (?, ?, ?);");

		while ((line = reader.readLine()) != null) {
			String artist = line.split("\t")[0];
			if (artist.matches("[A-Za-z0-9\\s\\p{Punct}]+")) {
				String gender = line.split("\t")[1];
				String value = line.split("\t")[2];
				prep.setString(1, artist);
				prep.setString(2, gender);
				prep.setString(3, value);
				prep.addBatch();
			}

		}
		reader.close();

		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);

		conn.close();
	}

}
