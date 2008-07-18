package edu.umd.cloud9.debug;

import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class MapredHarness {

	public static InMemoryOutputCollector run(JobConf conf) {
		InMemoryOutputCollector fPairs = new InMemoryOutputCollector();

		try {
			InMemoryOutputCollector iPairs = runMapper(conf);

			Reducer reducer = conf.getReducerClass().newInstance();

			Iterator<WritableComparable> keyIter = iPairs.getUniqueKeys();
			while (keyIter.hasNext()) {
				WritableComparable key = keyIter.next();
				reducer.reduce(key, iPairs.getValues(key), fPairs, Reporter.NULL);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return fPairs;
	}

	public static InMemoryOutputCollector runMapper(JobConf conf) {
		InMemoryOutputCollector collector = new InMemoryOutputCollector();

		try {
			// System.out.println(conf.getInputFormat());

			Path[] paths = conf.getInputPaths();

			if (paths.length > 1) {
				throw new RuntimeException("Error: currently supports only one input path!");
			}

			if (paths[0].getFileSystem(conf).getFileStatus(paths[0]).isDir()) {
				throw new RuntimeException(
						"Error: input path must be a SequenceFile, not a directory!");
			}

			Mapper mapper = conf.getMapperClass().newInstance();

			if (conf.getInputFormat().getClass().equals(SequenceFileInputFormat.class)) {
				mapOverSequenceFile(conf, paths[0], mapper, collector);
			} else if (conf.getInputFormat().getClass().equals(TextInputFormat.class)) {
				mapOverTextFile(conf, paths[0], mapper, collector);
			} else {
				throw new RuntimeException("Error: currently unsupported InputFormat!");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return collector;
	}

	public static void mapOverSequenceFile(JobConf conf, Path path, Mapper mapper,
			OutputCollector collector) throws Exception {
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);

		WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
		Writable value = (Writable) reader.getValueClass().newInstance();

		while (reader.next(key, value)) {
			// System.out.println(key + ": " + value);
			mapper.map(key, value, collector, Reporter.NULL);
		}
		reader.close();

	}

	public static void mapOverTextFile(JobConf conf, Path path, Mapper mapper,
			OutputCollector collector) throws Exception {

		long len = path.getFileSystem(conf).getFileStatus(path).getLen();

		// System.out.println("length of file: " + len);
		LineRecordReader reader = new LineRecordReader(path.getFileSystem(conf).open(path), 0, len);

		LongWritable key = reader.createKey();
		Text value = reader.createValue();

		while (reader.next(key, value)) {
			// System.out.println(key + ": " + value);
			mapper.map(key, value, collector, Reporter.NULL);
		}

		reader.close();

	}
}
