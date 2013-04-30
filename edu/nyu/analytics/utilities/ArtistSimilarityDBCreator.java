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

public class ArtistSimilarityDBCreator {

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/mbox/artists.txt"));
		HashMap<Integer, String> id_artist = new HashMap<Integer, String>();
		HashMap<String, PriorityQueue<SimilarArtist>> similars = new HashMap<String, PriorityQueue<SimilarArtist>>();
		String line = "";

		while ((line = reader.readLine()) != null) {
			if (line.split(",").length == 2 && line.split(",")[1].matches("[0-9]+")) {
				String artist = line.split(",")[0];
				Integer number = Integer.parseInt(line.split(",")[1]);
				id_artist.put(number, artist);
			}
		}
		reader.close();

		Statement stat;
		PreparedStatement prep;
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:/home/shobit/development/big-data-project/subsets/mbox/similar_artists.db");
		stat = conn.createStatement();
		stat.executeUpdate("drop table if exists similar;");
		stat.executeUpdate("CREATE TABLE similar (\"artist_name\" TEXT NOT NULL, \"artist_similar\" TEXT NOT NULL, \"value\" TEXT NOT NULL);");
		prep = conn.prepareStatement("insert into similar values (?, ?, ?);");

		reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/mboxcf/part-r-00000"));

		line = "";
		while ((line = reader.readLine()) != null) {
			String artist = id_artist.get(Integer.parseInt(line.split("\t")[0]));
			String similar = id_artist.get(Integer.parseInt(line.split("\t")[1]));
			String val = line.split("\t")[2];

			if (artist == null || similar == null) {
				continue;
			}
			if (!similars.containsKey(artist)) {
				PriorityQueue<SimilarArtist> sim = new PriorityQueue<SimilarArtist>();
				similars.put(artist, sim);
			}

			PriorityQueue<SimilarArtist> sim = similars.get(artist);
			sim.add(new SimilarArtist(similar, Float.parseFloat(val)));
			if (sim.size() > 20) {
				sim.poll();
			}
			similars.put(artist, sim);
		}

		for (Entry<String, PriorityQueue<SimilarArtist>> entry : similars.entrySet()) {
			PriorityQueue<SimilarArtist> sim = entry.getValue();
			for (SimilarArtist sa : sim) {
				prep.setString(1, entry.getKey());
				prep.setString(2, sa.getArtist());
				prep.setString(3, "" + sa.getValue());
				prep.addBatch();
			}
		}

		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);

		conn.close();
	}
}
