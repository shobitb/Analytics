package edu.nyu.analytics.utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class SeqFileCreatorHelper {

	public static List<String> tags = new ArrayList<String>();

	public static HashMap<String, HashMap<String, Float>> map = new HashMap<String, HashMap<String, Float>>();

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

		HashMap<String, String> artistNameId = new HashMap<String, String>();
		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/MillionSongSubset/AdditionalFiles/subset_unique_artists.txt"));
		String line = "";
		while ((line = reader.readLine()) != null) {
			String name = line.split(",")[3];
			String id = line.split(",")[0];
			artistNameId.put(name, id);
		}

		System.out.println("Inside");
		Statement statement = dbConnect();

		reader.close();

		reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/MillionSongSubset/AdditionalFiles/subset_unique_mbtags.txt"));
		line = "";
		while ((line = reader.readLine()) != null) {
			if (line.matches("[a-zA-Z0-9\\p{Punct}\\s]+")) {
				tags.add(line.trim());
			}
		}
		reader.close();

		reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/jobs/user-artist-freq/part-r-00000"));
		line = "";
		while ((line = reader.readLine()) != null) {
			if (map.size() > 2000) {
				System.out.println("writing to file...");
				BufferedWriter writer = new BufferedWriter(new FileWriter("/home/shobit/development/big-data-project/sequence_helper", true));
				for (Entry<String, HashMap<String, Float>> entry : map.entrySet()) {
					String user = entry.getKey();
					HashMap<String, Float> tmp = entry.getValue();
					writer.write(user + "\t");
					for (int i = 0; i < tags.size(); i++) {
						if (tmp.containsKey(tags.get(i))) {
							writer.write(tmp.get(tags.get(i)) + "\t");
						} else {
							writer.write("0.0\t");
						}
					}
					writer.newLine();
				}
				map.clear();
				writer.close();
				System.out.println("...done writing");
			}
			String[] split = line.split("\t");
			String user = split[0];
			HashMap<String, Float> tmp = new HashMap<String, Float>();
			for (int i = 1; i < split.length; i++) {
				String artist = artistNameId.get(split[i].split(",")[0]);
				float listens = Float.parseFloat(split[i].split(",")[1]);
				if (statement != null) {
					ResultSet rs = statement.executeQuery("SELECT mbtag FROM artist_mbtag WHERE artist_id='" + artist + "'");
					while (rs.next()) {
						String mbtag = rs.getString(1);
						if (mbtag.matches("[a-zA-Z0-9\\p{Punct}\\s]+")) {
							if (tmp.containsKey(mbtag)) {
								tmp.put(mbtag, tmp.get(mbtag) + listens);
							} else {
								tmp.put(mbtag, listens);
							}
						}
					}
				}
			}
			map.put(user, tmp);
		}
		reader.close();
	}

	public static Statement dbConnect() throws ClassNotFoundException {

		Class.forName("org.sqlite.JDBC");
		Connection connection = null;

		try {
			// create a database connection
			connection = DriverManager.getConnection("jdbc:sqlite:/home/shobit/development/big-data-project/subsets/MillionSongSubset/AdditionalFiles/subset_artist_term.db");
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
