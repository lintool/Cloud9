package edu.umd.hooka;

import edu.umd.hooka.alignment.IndexedFloatArray;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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


public class PhraseExtractAndCount {
	public static class MapClass1 extends MapReduceBase implements Mapper<IntWritable, PhrasePair, PhrasePair, IntWritable>
	{
		    
		private final static IntWritable one = new IntWritable(1);
		    
		public void map(IntWritable key, PhrasePair value, 
				OutputCollector<PhrasePair, IntWritable> output, 
				Reporter reporter) throws IOException {
			ArrayList<PhrasePair> extracts = value.extractConsistentPhrasePairs(7);
			for (PhrasePair p : extracts) {
				output.collect(p, one);
		    	}
		    }
		}
	  
	public static class MapClass2 extends MapReduceBase
		implements Mapper<PhrasePair, IntWritable, PhrasePair, IntWritable> {
		    
		private final static Phrase empty = new Phrase();
		    
		MapClass2() {
			super();
			empty.setLanguage(0);
		}
		    
		public void map(PhrasePair key, IntWritable value, 
				OutputCollector<PhrasePair, IntWritable> output, 
				Reporter reporter) throws IOException {
			PhrasePair k = new PhrasePair(key.getF(), key.getE());
			Phrase e = k.getE();
			k.setAlignment(null);
			k.setE(empty);
			output.collect(k, value);
			k.setF(e);
			output.collect(k, value);
		}
	}

	public static class MapClass3 extends MapReduceBase
	  implements Mapper<PhrasePair, IntWritable, PhrasePair, IntWritable> {
		    		    		    
		public void map(PhrasePair key, IntWritable value, 
				OutputCollector<PhrasePair, IntWritable> output, 
				Reporter reporter) throws IOException {
			if (key.getE().size() == 0) {
				output.collect(key, value);
		    } else {
		    	output.collect(key, value);
		    	PhrasePair swapped = key.getTranspose();
		    	output.collect(swapped, value);
		    }
		}
	}

	public static class MapClass4 extends MapReduceBase
	  	implements Mapper<PhrasePair, FloatWritable, PhrasePair, IndexedFloatArray> {
		    
		IndexedFloatArray scores = new IndexedFloatArray(2);
		  
		public void map(PhrasePair key, FloatWritable value, 
				OutputCollector<PhrasePair, IndexedFloatArray> output, 
				Reporter reporter) throws IOException {
			float v = value.get();
			if (key.getF().getLanguage() == 0) {
				PhrasePair swapped = key.getTranspose();
				scores.set(0, 0.0f);
				scores.set(1, v);
				output.collect(swapped, scores);
			} else {
				scores.set(0, v);
				scores.set(1, 0.0f);
				output.collect(key, scores);
			}
		}
	}
	  
	public static class Reduce extends MapReduceBase
	  implements Reducer<PhrasePair, IntWritable, PhrasePair, IntWritable> {
		IntWritable res = new IntWritable();
		public void reduce(PhrasePair key, Iterator<IntWritable> values,
				OutputCollector<PhrasePair,IntWritable> output, 
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			res.set(sum);
			output.collect(key, res);
		}
	}
		  	
	public static class ReducePT extends MapReduceBase
	  implements Reducer<PhrasePair, IntWritable, PhrasePair, FloatWritable> {
			    
		float marginal = 0.0f;
		int need_to_cover = 0;
		FloatWritable prob = new FloatWritable(0.0f);
			  
		public void reduce(PhrasePair key, Iterator<IntWritable> values,
				OutputCollector<PhrasePair, FloatWritable> output, 
				Reporter reporter) throws IOException {
			if (!values.hasNext())
				throw new UnexpectedException("no values for " + key);
			int v = values.next().get();
			if (need_to_cover == 0) {
				if (key.getE().size() != 0)
					throw new UnexpectedException("Expected empty e-side: " + key);
				need_to_cover = v;
				if (v < 1)
					throw new UnexpectedException("Bad count: " + v);
				marginal = (float)v;
			} else {
				if (key.getE().size() == 0)
					throw new UnexpectedException("unaccounted for counts: " + need_to_cover + " key=" +key);
				float p = (float)v / marginal;
				prob.set(p);
				output.collect(key, prob);
				need_to_cover -= v;
			}			    
		}
	}

	public static class ReduceSumScores extends MapReduceBase
		  	implements Reducer<PhrasePair, IndexedFloatArray, PhrasePair, IndexedFloatArray> {
			    
		IndexedFloatArray scores = new IndexedFloatArray(2);
			  
		public void reduce(PhrasePair key, Iterator<IndexedFloatArray> values,
				OutputCollector<PhrasePair, IndexedFloatArray> output, 
				Reporter reporter) throws IOException {
			scores.clear();
			while (values.hasNext()) {
				scores.plusEquals(values.next());
			}
			output.collect(key, scores);
		}
	}
		  
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		CorpusInfo corpus =
			CorpusInfo.getCorpus(CorpusInfo.Corpus.ARABIC_500k);
		Path ppCountTemp= new Path("ppc.paircount.tmp");
		Path ppMarginalTemp = new Path("ppc.marginals.tmp");
		Path ppPtableTemp = new Path("ppc.ptable.tmp");

		int mapTasks    = 38;
		int reduceTasks = 38;

		JobConf conf = new JobConf(PhraseExtractAndCount.class);
		FileSystem fs = FileSystem.get(conf);
		
		fs.delete(ppCountTemp);
		fs.delete(ppMarginalTemp);
		fs.delete(ppPtableTemp);
		fs.delete(corpus.getLocalPhraseTable());

		conf.setJobName("PhraseExtractAndCount");
		 
		conf.setOutputKeyClass(PhrasePair.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(MapClass1.class);        
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		        
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		
		FileInputFormat.setInputPaths(conf, corpus.getAlignedBitext());
		FileOutputFormat.setOutputPath(conf, ppCountTemp);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		        
		JobClient.runJob(conf);

		conf = new JobConf(PhraseExtractAndCount.class);
		conf.setJobName("PhraseExtractAndCount_marginals");
		conf.setInputFormat(SequenceFileInputFormat.class);

		conf.setOutputKeyClass(PhrasePair.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass2.class);        
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		FileInputFormat.setInputPaths(conf, ppCountTemp);
		FileOutputFormat.setOutputPath(conf, ppMarginalTemp);		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		        
		JobClient.runJob(conf);
		    
		conf = new JobConf(PhraseExtractAndCount.class);
		conf.setJobName("PhraseExtractAndCount_ptscore");
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
			
		conf.setOutputKeyClass(PhrasePair.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputValueClass(FloatWritable.class);
		    
		conf.setMapperClass(MapClass3.class);      
		conf.setReducerClass(ReducePT.class);
		        
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		FileInputFormat.addInputPath(conf, ppCountTemp);
		FileInputFormat.addInputPath(conf, ppMarginalTemp);

		FileOutputFormat.setOutputPath(conf, ppPtableTemp);

		JobClient.runJob(conf);

		conf = new JobConf(PhraseExtractAndCount.class);
		conf.setJobName("PhraseExtractAndCount_ptcombine");
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setOutputKeyClass(PhrasePair.class);
		conf.setOutputValueClass(IndexedFloatArray.class);
		
		conf.setMapperClass(MapClass4.class);      
		conf.setReducerClass(ReduceSumScores.class);
		        
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		FileInputFormat.setInputPaths(conf, ppPtableTemp);
		FileOutputFormat.setOutputPath(conf, corpus.getLocalPhraseTable());
		JobClient.runJob(conf);

	}	  
}
