package edu.umd.cloud9.indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.util.Histogram;

public class LookupPostings {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [index-path] [collection-path]");
			System.exit(-1);
		}

		String indexPath = args[0];
		String collectionPath = args[1];

		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		MapFile.Reader reader = new MapFile.Reader(fs, indexPath + "/part-00000", config);

		FSDataInputStream collection = fs.open(new Path(collectionPath));
		BufferedReader d = new BufferedReader(new InputStreamReader(collection));

		Text key = new Text();
		ArrayListWritable<PairOfInts> value = new ArrayListWritable<PairOfInts>();

		System.out.println("Looking up postings for the term \"starcross'd\"");
		key.set("starcross'd");

		reader.get(key, value);

		for (PairOfInts pair : value) {
			System.out.println(pair);
			collection.seek(pair.getLeftElement());
			System.out.println(d.readLine());
		}

		key.set("gold");
		reader.get(key, value);

		Histogram<Integer> goldHist = new Histogram<Integer>();
		for (PairOfInts pair : value) {
			goldHist.count(pair.getRightElement());
		}

		System.out.println("histogram of tf values for gold");
		goldHist.printCounts();

		key.set("silver");
		reader.get(key, value);

		Histogram<Integer> silverHist = new Histogram<Integer>();
		for (PairOfInts pair : value) {
			silverHist.count(pair.getRightElement());
		}

		System.out.println("histogram of tf values for silver");
		silverHist.printCounts();

		key.set("bronze");
		Writable w = reader.get(key, value);

		if (w == null)
			System.out.println("the term bronze does not appear in the collection");

		collection.close();
		reader.close();
	}
}
