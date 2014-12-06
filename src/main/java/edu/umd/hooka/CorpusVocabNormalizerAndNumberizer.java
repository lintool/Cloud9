package edu.umd.hooka;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.streaming.StreamXmlRecordReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.hooka.alignment.aer.ReferenceAlignment;
import edu.umd.hooka.corpora.Chunk;
import edu.umd.hooka.corpora.Language;
import edu.umd.hooka.corpora.LanguagePair;
import edu.umd.hooka.corpora.ParallelChunk;
import edu.umd.hooka.corpora.ParallelCorpusReader;


public class CorpusVocabNormalizerAndNumberizer {
	static enum BitextCompilerCounters { EN_WORDS, FR_WORDS, CHUNKS, WRONG_LANGUAGE, SRC_TOO_LONG, TGT_TOO_LONG };
	private static final Logger sLogger = Logger.getLogger(CorpusVocabNormalizerAndNumberizer.class);

	static final String SRC_LANG = "ha.sourcelang";
	static final String TGT_LANG = "ha.targetlang";

	public static class BitextCompilerMapper extends MapReduceBase
	implements Mapper<Text, Text, Text, PhrasePair> {
		String outputBase = null;
		Path pf = null;
		Path pe = null;
		Path pa = null;
		static Vocab vocE = null;
		static Vocab vocF = null;
		ParallelCorpusReader pcr = new ParallelCorpusReader();
		Language src = null;
		Language tgt = null;
		AlignmentWordPreprocessor sawp = null;
		AlignmentWordPreprocessor tawp = null;
		LanguagePair lp = null;
		JobConf job_ = null;

		public void configure(JobConf job) {
			sLogger.setLevel(Level.OFF);
			src = Language.languageForISO639_1(job.get(SRC_LANG));
			tgt = Language.languageForISO639_1(job.get(TGT_LANG));
			sLogger.debug("Source language: "+src.code());
			sLogger.debug("Target language: "+tgt.code());
			
			boolean useVocabServer = false;
			if (!useVocabServer) {
				if (vocE == null) vocE = new VocabularyWritable();
				if (vocF == null) vocF = new VocabularyWritable();
			} else {
				try {
					vocE = new VocabServerClient(job.get("ha.vocabserver.host"),
							Integer.parseInt(job.get("ha.vocabserver.port1")));
					vocF = new VocabServerClient(job.get("ha.vocabserver.host"),
							Integer.parseInt(job.get("ha.vocabserver.port2")));
				} catch (IOException e) { e.printStackTrace(); throw new RuntimeException(e); }
			}
			lp = LanguagePair.languageForISO639_1Pair(
					src.code() + "-" + tgt.code());

			if(job.getBoolean("ha.trunc.use", true)){
				sawp = AlignmentWordPreprocessor.CreatePreprocessor(lp, src, job);
				tawp = AlignmentWordPreprocessor.CreatePreprocessor(lp, tgt, job);				
			}else{
				sawp = AlignmentWordPreprocessor.CreatePreprocessor(null, null, job);
				tawp = AlignmentWordPreprocessor.CreatePreprocessor(null, null, job);	
			}
			job_ = job;
		}

		public int[] convertStrings(String[] s, Vocab v) {
			int[] res = new int[s.length];
			for (int i =0; i<s.length; ++i) {
				res[i] = v.addOrGet(s[i]);
				sLogger.info(s[i]+"-->"+res[i]);
			}
			return res;
		}

		Text ok = new Text("");

		@Override
		public void close() {
			System.err.println("Target: " + vocE.size() + " types. Writing to "+job_.get("root",null)+"/vocab.E");
			System.err.println("Source: " + vocF.size() + " types .Writing to "+job_.get("root",null)+"/vocab.F");

			//write out vocabulary to file
			try {
				FileSystem fs = FileSystem.get(job_);

				DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(job_.get("root",null)+"/vocab.E"))));
				((VocabularyWritable) vocE).write(dos);
				dos.close();
				DataOutputStream dos2 = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(job_.get("root",null)+"/vocab.F"))));
				((VocabularyWritable) vocF).write(dos2);
				dos2.close();
				
			} catch (IOException e) {
				throw new RuntimeException("Vocab couldn't be written to disk.\n"+e.toString());
			}
		}

		//read in xml-format bitext and output each instance as a PhrasePair object with a unique string id as key.

		public void map(Text key, Text value, 
				OutputCollector<Text, PhrasePair> oc, 
				Reporter reporter) throws IOException {

			//key: a single sentence in both languages and alignment
			//ignore value. each key is parallel sentence and its alignment, in xml format

			ParallelChunk c = pcr.parseString(key.toString());
			ok.set(c.idString());
			
			//Chunk is an array of tokens in the sentence, without any special tokenization (just separated by spaces)
			Chunk fc = c.getChunk(src);
			Chunk ec = c.getChunk(tgt);
			if (fc == null || ec == null) {
				reporter.incrCounter(BitextCompilerCounters.WRONG_LANGUAGE, 1);
				return;
			}
			if (fc.getLength() > 200) {
				reporter.incrCounter(BitextCompilerCounters.SRC_TOO_LONG, 1);
				return;
			}
			if (ec.getLength() > 200) {
				reporter.incrCounter(BitextCompilerCounters.TGT_TOO_LONG, 1);
				return;
			}

			//ec,fc: English/French sentence represented as sequence of words
			//vocE,vocF: vocabularies for english and french, of type VocabularyWritable

			//ee,fe: integer representation of words in sentences ec and fc
			sLogger.debug("Target sentence:");
			int[] ee = convertStrings(tawp.preprocessWordsForAlignment(ec.getWords()), vocE);
			sLogger.debug("Source sentence:");
			int[] fe = convertStrings(sawp.preprocessWordsForAlignment(fc.getWords()), vocF);

			//e,f: phrase from whole sentence
			Phrase e = new Phrase(ee, 0);
			Phrase f = new Phrase(fe, 1);

			edu.umd.hooka.PhrasePair b = new PhrasePair(f,e);
			ReferenceAlignment ra = c.getReferenceAlignment(lp);
			if (ra != null) {
				b.setAlignment(ra);
			}
			reporter.incrCounter(BitextCompilerCounters.EN_WORDS, e.getWords().length);
			reporter.incrCounter(BitextCompilerCounters.FR_WORDS, f.getWords().length);
			reporter.incrCounter(BitextCompilerCounters.CHUNKS, 1);
			oc.collect(ok, b);
		}
	}	

	public static class XMLInput extends FileInputFormat<Text, Text> {
		private CompressionCodecFactory compressionCodecs = null;

		public void configure(JobConf conf) {
			compressionCodecs = new CompressionCodecFactory(conf);
		}

		protected boolean isSplitable(FileSystem fs, Path file) {
			if (compressionCodecs == null) return true;
			return compressionCodecs.getCodec(file) == null;
		}

		public RecordReader<Text, Text> getRecordReader(
				InputSplit genericSplit, JobConf job,
				Reporter reporter)
				throws IOException {

			reporter.setStatus(genericSplit.toString());
			FileSplit split = (FileSplit)genericSplit;
			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			if (compressionCodecs != null && compressionCodecs.getCodec(file) != null)
				throw new RuntimeException("Not handling compression!");

			return new StreamXmlRecordReader(fileIn, split, reporter, job, FileSystem.get(job));
		}

	}

	@SuppressWarnings({ "deprecation", "null" })
	public static void preprocessAndNumberizeFiles(Configuration c,
			String inputPaths, Path output) throws IOException {
		sLogger.setLevel(Level.INFO);
		
		JobConf conf = new JobConf(c);

		conf.setJobName("bitext.compile");

		boolean useVocabServer = false;

		Thread vst1= null;
		Thread vst2= null;
		VocabServer vocabServer1 = null;
		VocabServer vocabServer2 = null;
		try {
			//inputPaths = bi-text given as input in main method of HadoopAlign
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(PhrasePair.class);
			conf.setMapperClass(BitextCompilerMapper.class);
			conf.setReducerClass(IdentityReducer.class);
			conf.setNumMapTasks(1);
			conf.setNumReduceTasks(1);
			FileInputFormat.setInputPaths(conf, inputPaths);
			conf.set("stream.recordreader.begin", "<pchunk");
			conf.set("stream.recordreader.end", "</pchunk>");
			conf.set("stream.recordreader.slowmatch", "false");
			conf.set("stream.recordreader.maxrec", "100000");
			conf.setInputFormat(XMLInput.class);
			FileOutputFormat.setOutputPath(conf, output);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			conf.setJarByClass(CorpusVocabNormalizerAndNumberizer.class);
			System.out.println("Running job "+conf.getJobName());
			System.out.println("Input: " + inputPaths);
			System.out.println("Output: "+output);
			JobClient.runJob(conf);
		} finally {
			try {
				if (vst1!=null) vocabServer1.stopServer();
				if (vst2!=null) vocabServer2.stopServer();
				if (vst1!=null) vst1.join();
				if (vst2!=null) vst2.join();
			} catch (InterruptedException e) {}
		}
	}

	public static void main(String args[]) {
		Path[] files = new Path[2];
		files[0] = new Path("/Users/redpony/bitexts/man-align/deen.ccb_jhu.xml");
		files[1] = new Path("/tmp/bar.xml");
		try {
			Configuration c = new Configuration();
			c.set(SRC_LANG, "de");
			c.set(TGT_LANG, "en");
//			c.set("mapred.job.tracker", "local");
//			c.set("fs.default.name", "file:///");
//			FileSystem.get(c).delete(new Path("/Users/ferhanture/Documents/work/hadoop-0.20.1/dummy.out"), true);
//			preprocessAndNumberizeFiles(c, "/Users/ferhanture/edu/research/programs/hadoop-aligner/training-data.tar/eu-nc-wmt2008.de-en/eu-nc-wmt2008.de-en.xml", new Path("/Users/ferhanture/Documents/work/hadoop-0.20.1/dummy.out"));
			
			preprocessAndNumberizeFiles(c, "/umd-lin/fture/mt/eu-nc-wmt2008.de-en.xml", new Path("/umd-lin/fture/mt/aligner/comp-bitext"));

		} catch (Exception e) { e.printStackTrace(); }
	}
}
