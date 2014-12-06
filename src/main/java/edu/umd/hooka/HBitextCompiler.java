package edu.umd.hooka;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


public class HBitextCompiler {
	static enum BitextCompilerCounters { EN_WORDS, FR_WORDS, LINES, ENCODING_ERRORS };

	static final String OUTPUT_BASENAME = "bitextcomp.outputbasename";
	static final String EN_PATH         = "bitextcomp.enpath";
	static final String FR_PATH         = "bitextcomp.frpath";
	static final String AL_PATH         = "bitextcomp.alpath";
	public static class BitextCompilerMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, LongWritable, Text> {
		String outputBase = null;
		Path pf = null;
		Path pe = null;
		Path pa = null;
		public void configure(JobConf job) {
			outputBase = job.get((String)OUTPUT_BASENAME);
			pe = new Path(job.get((String)EN_PATH));
			pf = new Path(job.get((String)FR_PATH));
			String alps = job.get((String)AL_PATH);
			if (alps != null && alps.compareTo("") != 0)
				pa = new Path(alps);
		}
		
		public void map(LongWritable key, Text value, 
		                    OutputCollector<LongWritable, Text> oc, 
		                    Reporter reporter) throws IOException {
			Path output = new Path(outputBase);
			Path pmd = new Path(outputBase + ".metadata");
			
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			FileSystem fileSys = FileSystem.get(conf);
			
			VocabularyWritable vocE = new VocabularyWritable();
			VocabularyWritable vocF = new VocabularyWritable();
			org.apache.hadoop.io.SequenceFile.Writer sfw = 
				SequenceFile.createWriter(fileSys, conf, output, IntWritable.class, PhrasePair.class);

			//if (true) throw new RuntimeException("here " + pe + " " + pf + " " + pa);
			
			boolean hasAlignment = (pa != null);
			BufferedReader rde = new BufferedReader(new InputStreamReader(fileSys.open(pe), "UTF8"));
			BufferedReader rdf = new BufferedReader(new InputStreamReader(fileSys.open(pf), "UTF8"));
			BufferedReader rda = null;
			if (hasAlignment)
				rda = new BufferedReader(new InputStreamReader(fileSys.open(pa), "UTF8"));
			String es;
			IntWritable lci = new IntWritable(0);
			int lc = 0;
			reporter.incrCounter(BitextCompilerCounters.ENCODING_ERRORS, 0);
			while ((es = rde.readLine()) != null) {
				lc++;
				if (lc % 100 == 0) reporter.progress();
				reporter.incrCounter(BitextCompilerCounters.LINES, 1);
				String fs = rdf.readLine();
				if (fs == null) {
					throw new RuntimeException(pf + " has fewer lines than " + pe);
				}
				try {
					Phrase e=Phrase.fromString(0, es, vocE);
					Phrase f=Phrase.fromString(1, fs, vocF);
					PhrasePair b = new PhrasePair(f,e);
					if (hasAlignment) {
						Alignment a = new Alignment(f.size(), e.size(),
								rda.readLine());
						b.setAlignment(a);
					}
					lci.set(lc);
					sfw.append(lci, b);
					reporter.incrCounter(BitextCompilerCounters.EN_WORDS, e.getWords().length);
					reporter.incrCounter(BitextCompilerCounters.FR_WORDS, f.getWords().length);
					reporter.progress();
				} catch (Exception e) {
					System.err.println("\nAt line "+lc+" caught: "+e);
					reporter.incrCounter(BitextCompilerCounters.ENCODING_ERRORS, 1);
				}
			}
			if (rdf.readLine() != null) {
				throw new RuntimeException(pf + " has more lines than " + pe);
			}
			sfw.close();
			Path pve = new Path(outputBase + ".voc.e");
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fileSys.create(pve)));
			vocE.write(dos);
			dos.close();
			Path pvf = new Path(outputBase + ".voc.f");
			dos = new DataOutputStream(new BufferedOutputStream(fileSys.create(pvf)));
			vocF.write(dos);
			dos.close();
			Metadata theMetadata = new Metadata(lc, vocE.size(), vocF.size());
			ObjectOutputStream mdstream = new ObjectOutputStream(new BufferedOutputStream(fileSys.create(pmd)));
			mdstream.writeObject(theMetadata);
			mdstream.close();
			oc.collect(new LongWritable(0), new Text("done"));
		}
	}	

	/**
	 * @param args
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		JobConf conf = new JobConf(HBitextCompiler.class);
		conf.set(OUTPUT_BASENAME, "/shared/bitexts/ep700k+nc.de-en/ep700k+nc");
		conf.set(FR_PATH, "filt.lc.de");
		conf.set(EN_PATH, "filt.lc.en");
		conf.set(AL_PATH, ""); ///user/redpony/model-5M/aligned.grow-diag-final");
		conf.setJobName("bitext.compile");
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(BitextCompilerMapper.class); 
		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(conf, new Path("dummy"));
		try {
			FileSystem.get(conf).delete(new Path("dummy.out"));
			FileOutputFormat.setOutputPath(conf, new Path("dummy.out"));
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			JobClient.runJob(conf);
		} catch (IOException e) {
			System.err.println("Caught " + e);
			e.printStackTrace();
		}
	}

}
