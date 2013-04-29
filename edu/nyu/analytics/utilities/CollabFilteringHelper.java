package edu.nyu.analytics.utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class CollabFilteringHelper {

	public static void main(String[] args) throws IOException {
		HashMap<String, Integer> users = new HashMap<String, Integer>();
		HashMap<String, Integer> tracks = new HashMap<String, Integer>();
		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/subsets/user-artist-freq/part-r-00000"));
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/shobit/development/big-data-project/subsets/user-artist-freq/cf.csv", true));
		BufferedWriter writer2 = new BufferedWriter(new FileWriter("/home/shobit/development/big-data-project/subsets/user-artist-freq/users.txt", true));
		BufferedWriter writer3 = new BufferedWriter(new FileWriter("/home/shobit/development/big-data-project/subsets/user-artist-freq/artists.txt", true));
		String line = "";
		int i = 1;
		while ((line = reader.readLine()) != null) {
			System.out.println(i++);
			String[] split = line.split(",");
			boolean index1 = users.containsKey(split[0]);
			boolean index2 = tracks.containsKey(split[1]);
			if (index1 == false) {
				users.put(split[0], users.size());
			}
			if (index2 == false) {
				tracks.put(split[1], tracks.size());
			}
			writer.write("\n" + users.get(split[0]) + "," + tracks.get(split[1]) + "," + split[2]);
		}

		System.out.println("Writing users");
		for (Entry<String, Integer> entry : users.entrySet()) {
			writer2.write(entry.getKey() + "," + entry.getValue());
			writer2.newLine();
		}

		System.out.println("Writing tracks");
		for (Entry<String, Integer> entry : tracks.entrySet()) {
			writer3.write(entry.getKey() + "," + entry.getValue());
			writer3.newLine();
		}

	}
}
