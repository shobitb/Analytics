package edu.nyu.analytics.jobs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import ncsa.hdf.object.h5.H5File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EnergyDistribution {

	static Statement stat;
	static H5File h5;
	static Configuration config = HBaseConfiguration.create();
	static HTable table_energyDistribution;
	static PreparedStatement prep;
	static HashMap<Artist, ArrayList<Integer>> map = new HashMap<Artist, ArrayList<Integer>>();

	static class EnergyDistributionMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {

				String restPath = "/Users/hiral/Documents/RealTimeBigData/Data/";
				String o = value.toString();

				String filePath = new StringBuilder(restPath).append(o).toString();

				h5 = new H5File(filePath, H5File.READ);
				String artistid = HDF5Getters.get_artist_id(h5);
				String name = HDF5Getters.get_artist_name(h5);
				
				Artist artist = new Artist(artistid, name);

				if (!map.containsKey(artist)) {
					ArrayList<Integer> list = new ArrayList<Integer>(10);
					
					for(int i=0;i<10;i++)
						list.add(0);
					
					map.put(artist, list);
				}

				double energy = HDF5Getters.get_song_hotttnesss(h5);
				double temp = energy * 100;

				if (!Double.isNaN(energy)) {

					int col = (int) (temp / 11.0);

					ArrayList<Integer> list = map.get(artist);
					int t = list.get(col);

					t++;
					list.set(col, t);
					map.put(artist, list);

				}

			} catch (Exception e) {
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
		stat = conn.createStatement();
		stat.executeUpdate("drop table if exists artistenergy;");
		stat.executeUpdate("CREATE TABLE artistenergy ( \"artist_id\" TEXT NOT NULL, \"artist_name\" TEXT, \"0\" TEXT, \"1\" TEXT, "
				+ "\"2\" TEXT, \"3\" TEXT, \"4\" TEXT, \"5\" TEXT, \"6\" TEXT, \"7\" TEXT, \"8\" TEXT, \"9\" TEXT );");
		prep = conn.prepareStatement("insert into artistenergy values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
		
		for(Entry<Artist, ArrayList<Integer>> entry : map.entrySet()) {
			String id = entry.getKey().artist_id;
			String name = entry.getKey().arist_name;
			prep.setString(1, id);
			prep.setString(2, name);
			int k = 3;
			for(int i : entry.getValue()) {
				prep.setInt(k, i);
				k++;
			}
			prep.addBatch();
		}
		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);

		conn.close();
		
	}

}
