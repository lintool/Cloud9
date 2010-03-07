package edu.umd.cloud9.collection.wikipedia;

import ivory.lsh.data.PairOfIntSignature64;
import ivory.sentiment.HTMLParser;
import ivory.util.RetrievalEnvironment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import joshua.corpus.Corpus;
import joshua.corpus.suffix_array.ParallelCorpusGrammarFactory;
import joshua.corpus.vocab.SymbolTable;
import joshua.prefix_tree.ExtractRules;
import joshua.prefix_tree.Node;
import joshua.prefix_tree.PrefixTree;

import opennlp.maxent.Context;
import opennlp.maxent.GISModel;
import opennlp.tools.lang.german.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.io.PairOfStringInt;

public class ParseWikiData {
	public static final Logger sLogger = Logger.getLogger(ParseWikiData.class);
	protected static int NUM_PREDS;

	static enum Maps{
		Matched, SKIPPED;
	}

	public ParseWikiData(){

	}

	/**
	 * 
	 * 
	 * @author ferhanture
	 *
	 */
	static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, WikipediaPage, PairOfInts, Text> {

		private DocnoMapping mDocMapping;
		//		static SentenceDetectorME sdetector;

		public void configure(JobConf job) {
//			sLogger.setLevel(Level.DEBUG);
			mDocMapping = new WikipediaDocnoMapping();
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
				String indexPath = job.get("IndexPath");
				String mappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);
				mDocMapping.loadMapping(new Path(mappingFile), fs);
			} catch (IOException e) {
				e.printStackTrace();
			}
			Path[] localFiles;
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				throw new RuntimeException("Local cache files not read properly.");
			}
			//			try {
			//				sdetector = new SentenceDetector("/umd-lin/fture/mt/joshua/sentenceModel.bin");
			//			} catch (IOException e) {
			//				throw new RuntimeException("Sentence detector not created!"+localFiles[0].toString());
			//			}
			//			try {
			//				fs = FileSystem.get(job);
			//				sdetector = createSentenceDetector(fs,new Path("/umd-lin/fture/mt/joshua/sentenceModel.bin"));
			//
			//			} catch (IOException e) {
			//				e.printStackTrace();
			//			}

		}

		public void map(LongWritable key, WikipediaPage doc,
				OutputCollector<PairOfInts, Text> output, Reporter reporter)
		throws IOException {
			String docid = doc.getDocid();

			int docno = mDocMapping.getDocno(docid);
			if(docno<0){
				reporter.incrCounter(Maps.SKIPPED, 1);
				return;
			}
			
			String rawtext = doc.getContent();
			if(rawtext!=null){
				//				sLogger.debug("@RAW");
				//				sLogger.debug(rawtext);

				//								String[] sentences =  (WikipediaPage.parseAndCleanPage(rawtext)).split("[\\.\\?\\!]");

				//				sLogger.debug("@SENTENCES");
				//				int cnt = 0;
				String[] lines = rawtext.split("\n");
				//				for(String line : lines){
				//					Pattern p1 = Pattern.compile("\\<p\\>(.+)\\</p\\>");
				//					Matcher m1 = p1.matcher(line);
				//					if(m1.find()){
				//						reporter.incrCounter(Maps.Matched, 1);
				//						sLogger.debug(line);
				//						cnt++;
				//					}
				//				}
				//				if(cnt==0){
				//					sLogger.debug(rawtext);
				//				}
				sLogger.debug(rawtext);
				
				int i=0;
				for(String line : lines){
					//					String[] sentences = sdetector.sentDetect(line);
					String[] sentences =  (WikipediaPage.parseAndCleanPage(line)).split("[\\.\\?\\!]");
					for(String sentence : sentences){
						sentence = sentence.trim();
						if(sentence!=null && !sentence.equals("")){
							sentence = sentence.toLowerCase();
							sLogger.debug(docno+","+i+":"+sentence);

							//TOKENIZE AND LOWERCASE
							//						StringTokenizer tokenizer = new StringTokenizer(sentence.toString());
							//						String tokenized = "";
							//						while(tokenizer.hasMoreTokens()){
							//							tokenized+=tokenizer.nextToken().toLowerCase()+" ";
							//						}

							//							output.collect(new PairOfInts(docno,i), new Text(sentence));
							output.collect(new PairOfInts(docno,i), new Text(sentence));
							i++;
						}
					}
				}
				sLogger.debug("=======================");

			}
		}
	}

	public static class MyPartitioner implements Partitioner<PairOfInts,Text>{
		static int numDocs;

		public int getPartition(PairOfInts key,Text value,int numReducers){
			int bucketSize = numDocs/numReducers;
			int bucketNo = key.getLeftElement()/bucketSize;
			return ((bucketNo>=numReducers-1) ? (numReducers-1) : bucketNo);
		}

		public void configure(JobConf conf) {			
			numDocs = conf.getInt("NumDocs", -1);
		}
	}

	public static class MyReducer extends MapReduceBase implements
	Reducer<PairOfInts, Text, Text, Text> {
		Text nullText = new Text("");
		SequenceFile.Writer writer;
		FileSystem fs;
		JobConf mConf;
		String numSentsPath;
		int prevDocno, prevSentno;
		static String modelDir, ruleDir, grammarFile;
		static SymbolTable sourceVocab;
		static PrefixTree prefixTree;
		static ParallelCorpusGrammarFactory parallelCorpus;
		static ExtractRules extractRules;
		FSDataOutputStream outStream;
		
		public void configure(JobConf conf){
			sLogger.setLevel(Level.DEBUG);
			Path[] localFiles;

			
			modelDir = conf.get("modelDir");
			ruleDir = conf.get("ruleDir");

			extractRules = new ExtractRules();
            try {
				fs  = FileSystem.get(conf);
                localFiles = DistributedCache.getLocalCacheFiles(conf);
    			extractRules.setFS(FileSystem.getLocal(conf));
            } catch (IOException e) {
                throw new RuntimeException("Local cache files not read properly.");
            }
			extractRules.setFiles(localFiles[0].toString(), localFiles[1].toString(), localFiles[2].toString(), localFiles[2].toString(), localFiles[3].toString(), localFiles[4].toString(), localFiles[5].toString());
//            extractRules.setJoshDir(modelDir);
			
			parallelCorpus = null;
			try {
				parallelCorpus = extractRules.getGrammarFactory();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("parallel corpus not created");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("parallel corpus not created");
			}

			sLogger.info(parallelCorpus);
			Corpus sourceCorpus = parallelCorpus.getSourceCorpus();
			sourceVocab = sourceCorpus.getVocabulary();
		
			//create prefix tree and set parameters
			prefixTree = new PrefixTree(parallelCorpus);
			prefixTree.sentenceInitialX = extractRules.sentenceInitialX;
			prefixTree.sentenceFinalX   = extractRules.sentenceFinalX;
			prefixTree.edgeXMayViolatePhraseSpan = extractRules.edgeXViolates;

			prevDocno = -1;
			prevSentno = -1;
			numSentsPath = conf.get("NumSentsPath");
			mConf = conf;
		}

		public void reduce(PairOfInts docnosentno, Iterator<Text> sentence, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			int docno = docnosentno.getLeftElement();
			int sentno = docnosentno.getRightElement();
			if(writer==null){
				writer = SequenceFile.createWriter(fs, mConf, new Path(numSentsPath+"/"+docno+".txt"), IntWritable.class, IntWritable.class);
			}
			if(fs==null){
	            outStream = fs.create(new Path(ruleDir+"/"+docno+".txt"));
				prefixTree.setPrintStream(outStream);
			}
			
			if(prevDocno!=-1 && docno!=prevDocno){
				sLogger.debug("WRITTEN: "+prevDocno+","+(prevSentno+1));
				writer.append(new IntWritable(prevDocno), new IntWritable(prevSentno+1));	//ids start from zero, therefore the +1
			}
			sLogger.debug(docno+","+sentno);
			int count=0;
			while(sentence.hasNext()){
				boolean done = false;
				String sent = sentence.next().toString();
				int[] words = sourceVocab.getIDs(sent);
				while(!done){
					sLogger.info("trying "+sent);
					try {
						prefixTree.add(words);
						done = true;
//						reporter.incrCounter(Counter.ADDED, 1);
					} catch (OutOfMemoryError e) {
//						reporter.incrCounter(Counter.OUTOFMEMORY, 1);
						sLogger.info("Out of memory - attempting to clear cache to free space");
						parallelCorpus.getSuffixArray().getCachedHierarchicalPhrases().clear();
						prefixTree = null;
						System.gc();
						sLogger.info("Cleared cache and collected garbage. Now attempting to re-construct prefix tree...");
						Node.resetNodeCounter();
						prefixTree = new PrefixTree(parallelCorpus);
						prefixTree.setPrintStream(outStream);
						prefixTree.sentenceInitialX = extractRules.sentenceInitialX;
						prefixTree.sentenceFinalX   = extractRules.sentenceFinalX;
						prefixTree.edgeXMayViolatePhraseSpan = extractRules.edgeXViolates;
					}		
				}				
				count++;
				if(count>1){
					throw new RuntimeException("two sentences with same id");
				}
				output.collect(new Text(sent), nullText);
			}
			prevDocno = docno;
			prevSentno = sentno;
		}
		
		public void close() throws IOException {
			if(writer!=null){
				writer.append(new IntWritable(prevDocno), new IntWritable(prevSentno+1));	//ids start from zero, therefore the +1
				writer.close();
			}
			sLogger.debug("exit mapper");
		}

	}


	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		//		sLogger.setLevel(Level.DEBUG);

		sLogger.info("Parsing wiki data...");

		int mapTasks = 100;

		JobConf conf = new JobConf(ParseWikiData.class);
		conf.setJobName("ParseWikiData");
		FileSystem fs = FileSystem.get(conf);

		String collectionPath = args[0];	//"/umd/collections/wikipedia.raw/dewiki-20081206-pages-articles.xml";
		String indexPath = args[1];
		String outputPath = args[2];		//"/user/ferhan/mt/wiki.parsed2/";
		String numSentsPath = args[3];
		int numFiles = Integer.parseInt(args[4]);

		sLogger.info("Parsed wiki data to be stored in " + outputPath);
		sLogger.info("CollectionPath: " + collectionPath);
		sLogger.info("Number of docs: "+RetrievalEnvironment.readCollectionDocumentCount(fs, indexPath));
		
		fs.delete(new Path(outputPath), true);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(numFiles);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapred.map.max.attempts", 10);
		conf.setInt("mapred.reduce.max.attempts", 10);
		conf.setInt("mapred.task.timeout", 60000000);
		conf.set("IndexPath", indexPath);
		conf.set("NumSentsPath", numSentsPath);

		conf.setSpeculativeExecution(false);
		
		conf.setInt("NumDocs", RetrievalEnvironment.readCollectionDocumentCount(fs, indexPath)/1000);

//		DistributedCache.addCacheFile(new URI("/umd-lin/fture/mt/joshua/sentenceModel.bin"), conf);
		conf.set("modelDir", args[5]);
		conf.set("ruleDir", args[6]);

		DistributedCache.addCacheFile(new URI(args[5]+"/source.corpus"), conf);
		DistributedCache.addCacheFile(new URI(args[5]+"/target.corpus"), conf);
		DistributedCache.addCacheFile(new URI(args[5]+"/common.vocab"), conf);
		DistributedCache.addCacheFile(new URI(args[5]+"/source.suffixes"), conf);
		DistributedCache.addCacheFile(new URI(args[5]+"/target.suffixes"), conf);
		DistributedCache.addCacheFile(new URI(args[5]+"/alignment.grids"), conf);
		
		FileInputFormat.setInputPaths(conf, new Path(collectionPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setMapOutputKeyClass(PairOfInts.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setPartitionerClass(MyPartitioner.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		JobClient.runJob(conf);

	}

	public static SentenceDetectorME createSentenceDetector(FileSystem fs, Path path) {
		GISModel model = null;
		try {
			FSDataInputStream in = fs.open(path);
			String modelType = in.readUTF();
			if (!modelType.equals("GIS"))
				System.out.println("Error: attempting to load a "+modelType+
						" model as a GIS model."+
				" You should expect problems.");
			int correctionConstant = in.readInt();
			double correctionParam = in.readDouble();
			int numOutcomes = in.readInt();
			String[] outcomeLabels = new String[numOutcomes];
			for (int i=0; i<numOutcomes; i++) outcomeLabels[i] = in.readUTF();
			int numOCTypes =  in.readInt();
			int[][] outcomePatterns = new int[numOCTypes][];
			for (int i=0; i<numOCTypes; i++) {
				StringTokenizer tok = new StringTokenizer( in.readUTF(), " ");
				int[] infoInts = new int[tok.countTokens()];
				for (int j = 0; tok.hasMoreTokens(); j++) {
					infoInts[j] = Integer.parseInt(tok.nextToken());
				}
				outcomePatterns[i] = infoInts;
			}
			NUM_PREDS = in.readInt();
			String[] predLabels = new String[NUM_PREDS];
			for (int i=0; i<NUM_PREDS; i++)
				predLabels[i] = in.readUTF();
			Context[] params = new Context[NUM_PREDS];
			int pid=0;
			for (int i=0; i<outcomePatterns.length; i++) {
				//construct outcome pattern
				int[] outcomePattern = new int[outcomePatterns[i].length-1];
				for (int k=1; k<outcomePatterns[i].length; k++) {
					outcomePattern[k-1] = outcomePatterns[i][k];
				}
				//populate parameters for each context which uses this outcome pattern. 
				for (int j=0; j<outcomePatterns[i][0]; j++) {
					double[] contextParameters = new double[outcomePatterns[i].length-1];
					for (int k=1; k<outcomePatterns[i].length; k++) {
						contextParameters[k-1] = in.readDouble();
					}
					params[pid] = new Context(outcomePattern,contextParameters);
					pid++;
				}
			}
			model =  new GISModel(params,
					predLabels,
					outcomeLabels,
					correctionConstant,
					correctionParam);
		} catch (IOException e) {
			e.printStackTrace();
		}	


		return new SentenceDetectorME(model);
	}

}
