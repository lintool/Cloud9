package edu.umd.hooka.alignment;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import edu.umd.hooka.CorpusInfo;
import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.Vocab;

/**
 * General EM training framework for word alignment models.
 */
public class EFMarginalCounter {

	static CorpusInfo corpus =
		CorpusInfo.getCorpus(
				CorpusInfo.Corpus.ARABIC_5000k);
	
	public static class MarginalMapper extends MapReduceBase
	  implements Mapper<IntWritable,PhrasePair,IntWritable,IndexedFloatArray> {

		static final IntWritable emarginal = new IntWritable(0);
		static final IntWritable fmarginal = new IntWritable(1);

		int[] makeUnique(int[] x) {
			int cur = -1;
			int c = 0;
			for (int i : x) {
				if (i != cur) {
					c++;
					cur = i;
				}
			}
			int[] res = new int[c];
			cur = -1;
			c = 0;
			for (int i : x) {
				if (i != cur) {
					res[c] = i;
					c++;
					cur = i;
				}
			}
			return res;
		}
		OutputCollector<IntWritable, IndexedFloatArray> output_;
		float[] emap = new float[Vocab.MAX_VOCAB_INDEX];
		float[] fmap = new float[Vocab.MAX_VOCAB_INDEX];
		int maxF = -1;
		int maxE = -1;
		boolean hasValues = false;
			public void map(IntWritable key, PhrasePair value, 
				OutputCollector<IntWritable,IndexedFloatArray> output, 
				Reporter reporter) throws IOException {
			output_ = output;
			int[] es = value.getE().getWords();
			int[] fs = value.getF().getWords();
			Arrays.sort(es);
			Arrays.sort(fs);
			es = makeUnique(es);
			fs = makeUnique(fs);
			if (es[es.length - 1] > maxE) maxE = es[es.length - 1];
			if (fs[fs.length - 1] > maxF) maxF = fs[fs.length - 1];
			for (int e : es) emap[e] += 1.0f;
			for (int f : fs) fmap[f] += 1.0f;
			hasValues = true;
		}
		
		public IndexedFloatArray makeIFA(float[] map, int max) {
			int c = 0;
			for (int i = 0; i <= max; i++)
				if (map[i] > 0.5f) c++;
			int[] ind = new int[c];
			float[] vals = new float[c];
			c = 0;
			for (int i = 0; i <= max; i++)
				if (map[i] > 0.5f) {
					ind[c] = i;
					vals[c] = map[i];
					c++;
				}
			
			return new IndexedFloatArray(ind, vals);
		}
		
		@Override
		public void close() {
			try {
				if (hasValues) {
					output_.collect(emarginal, makeIFA(emap, maxE));
					output_.collect(fmarginal, makeIFA(fmap, maxF));
				}
			} catch (IOException e) {
				throw new RuntimeException("Caught " + e);
			}
		}
	}
	
	public static class MarginalReducer extends MapReduceBase
	  implements Reducer<IntWritable,IndexedFloatArray,IntWritable,IndexedFloatArray> {
		IntWritable oe = new IntWritable();
		public void reduce(IntWritable key, Iterator<IndexedFloatArray> values,
              OutputCollector<IntWritable,IndexedFloatArray> output, 
              Reporter reporter) throws IOException {
			IndexedFloatArray sum = new IndexedFloatArray();
			while (values.hasNext()) {
				sum.plusEqualsMismatchSize(values.next());
			}
			output.collect(key, sum);
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void computeMarginals(Path bitext, Path outputPath, int mappers) throws IOException {

		int reduceTasks = 2;
		JobConf conf = new JobConf(EFMarginalCounter.class);
			
		conf.setJobName("EFMarginals");
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IndexedFloatArray.class);
				    
		conf.setMapperClass(MarginalMapper.class);
		conf.setReducerClass(MarginalReducer.class);
				        
		conf.setNumMapTasks(mappers);
		conf.setNumReduceTasks(reduceTasks);
		FileInputFormat.setInputPaths(conf, corpus.getBitext());
		FileOutputFormat.setOutputPath(conf, outputPath);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
				
		JobClient.runJob(conf);

	}
		
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(EFMarginalCounter.class);
		FileSystem fileSys = FileSystem.get(conf);
		String sOutputPath="marginals";
		Path outputPath = new Path(sOutputPath);
		fileSys.delete(outputPath);
		computeMarginals(corpus.getBitext(), outputPath, 38);
	}
	
}
