package edu.nyu.analytics.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

public class SeqFileCreator {
	public static void main(String args[]) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/home/shobit/development/big-data-project/sequence_file");

		VectorWritable vec = new VectorWritable();

		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, VectorWritable.class);

		BufferedReader reader = new BufferedReader(new FileReader("/home/shobit/development/big-data-project/sequence_helper"));
		String line = "";

		while ((line = reader.readLine()) != null) {
			String[] split = line.split("\t");
			String userId = split[0];
			double[] arr = new double[split.length - 1];
			for (int i = 1; i < split.length; i++) {
				arr[i - 1] = Double.parseDouble(split[i]);
			}
			NamedVector user;
			user = new NamedVector(new DenseVector(arr), userId);
			vec.set(user);
			writer.append(new Text(user.getName()), vec);
		}
		reader.close();
		writer.close();
	}
}
