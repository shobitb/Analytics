package edu.nyu.analytics.utilities;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

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
			File pointsFolder = new File("/home/shobit/development/big-data-project/cluster/clusteredPoints");
			File files[] = pointsFolder.listFiles();
			bw = new BufferedWriter(new FileWriter("/home/shobit/development/big-data-project/cluster/mapReduceInput", true));
			for (File file : files) {
				if (!file.getName().matches("part-m.*"))
					continue;
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file.getAbsolutePath()), conf);
				IntWritable key = new IntWritable();
				WeightedVectorWritable value = new WeightedVectorWritable();
				while (reader.next(key, value)) {
					NamedVector vector = (NamedVector) value.getVector();
					String vectorName = vector.getName();
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