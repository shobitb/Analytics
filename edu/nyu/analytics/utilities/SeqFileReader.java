package edu.nyu.analytics.utilities;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.math.stat.clustering.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.Kluster;

public class SeqFileReader {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			BufferedWriter bw;
			long i = 0;
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			File pointsFolder = new File("/home/shobit/development/big-data-project/subsets/final-c");
			File files[] = pointsFolder.listFiles();
			bw = new BufferedWriter(new FileWriter("/home/shobit/development/big-data-project/subsets/final-c/output", true));
			for (File file : files) {
				if (!file.getName().matches("part-r.*"))
					continue;
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file.getAbsolutePath()), conf);
				IntWritable key = new IntWritable();
				ClusterWritable value = new ClusterWritable();
				while (reader.next(key, value)) {
					Kluster vector = (Kluster) value.getValue();
					String vectorName = vector.computeCentroid().toString();
					bw.write(vectorName + "\t" + key.toString() + "\n");
					System.out.println(i++);
				}
				reader.close();
			}
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}