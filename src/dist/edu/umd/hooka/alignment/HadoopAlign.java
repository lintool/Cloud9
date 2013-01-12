package edu.umd.hooka.alignment;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.hooka.Alignment;
import edu.umd.hooka.AlignmentPosteriorGrid;
import edu.umd.hooka.CorpusVocabNormalizerAndNumberizer;
import edu.umd.hooka.PServer;
import edu.umd.hooka.PServerClient;
import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.aer.ReferenceAlignment;
import edu.umd.hooka.alignment.hmm.ATable;
import edu.umd.hooka.alignment.hmm.HMM;
import edu.umd.hooka.alignment.hmm.HMM_NullWord;
import edu.umd.hooka.alignment.model1.Model1;
import edu.umd.hooka.alignment.model1.Model1_InitUniform;
import edu.umd.hooka.ttables.TTable;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

/**
 * General EM training framework for word alignment models.
 */
public class HadoopAlign {

  private static final Logger sLogger = Logger.getLogger(HadoopAlign.class);
  static boolean usePServer = false;
  static final String KEY_TRAINER = "ha.trainer";
  static final String KEY_ITERATION = "ha.model.iteration";
  static final String MODEL1_UNIFORM_INIT = "model1.uniform";
  static final String MODEL1_TRAINER = "model1.trainer";
  static final String HMM_TRAINER = "hmm.baumwelch.trainer";

  static public ATable loadATable(Path path, Configuration job) throws IOException {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(job);
    FileSystem fileSys = FileSystem.get(conf);

    DataInput in = new DataInputStream(new BufferedInputStream(fileSys.open(path)));
    ATable at = new ATable();
    at.readFields(in);

    return at;
  }

  static public Vocab loadVocab(Path path, Configuration job) throws IOException {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(job);
    FileSystem fileSys = FileSystem.get(conf);

    DataInput in = new DataInputStream(new BufferedInputStream(fileSys.open(path)));
    VocabularyWritable at = new VocabularyWritable();
    at.readFields(in);

    return at;
  }

  static public Vocab loadVocab(Path path, FileSystem fileSys) throws IOException {
    DataInput in = new DataInputStream(new BufferedInputStream(fileSys.open(path)));
    VocabularyWritable at = new VocabularyWritable();
    at.readFields(in);

    return at;
  }
  protected static class AEListener implements AlignmentEventListener {
    private Reporter r;
    public AEListener(Reporter rep) { r = rep; }
    public void notifyUnalignablePair(PhrasePair pp, String reason) {
      r.incrCounter(CrossEntropyCounters.INFINITIES, 1);
      System.err.println("Can't align " + pp);
    }
  }

  public static enum AlignmentEvalEnum {
    SURE_HITS,
    PROBABLE_HITS,
    HYPOTHESIZED_ALIGNMENT_POINTS,
    REF_ALIGNMENT_POINTS,
  }

  public static class AlignmentBase extends MapReduceBase {
    Path ltp = null;
    AlignmentModel trainer = null;
    boolean useNullWord = false;
    boolean hasCounts = false;
    String trainerType = null;
    int iteration = -1;
    HadoopAlignConfig job = null;
    FileSystem ttfs = null;
    TTable ttable = null;
    boolean generatePosteriors = false;
    public void configure(JobConf j) {
      job = new HadoopAlignConfig(j);
      generatePosteriors = j.getBoolean("ha.generate.posteriors", false);
      try { ttfs = FileSystem.get(job); }
      catch (IOException e) { throw new RuntimeException("Caught " + e); }
      Path[] localFiles = null;
      /*try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
				ttfs = FileSystem.getLocal(job);
			} catch (IOException e) {
				throw new RuntimeException("Caught: " + e);
			}*/
      trainerType = job.get(KEY_TRAINER);
      if (trainerType == null || trainerType.equals(""))
        throw new RuntimeException("Missing key: " + KEY_TRAINER);
      String it = job.get(KEY_ITERATION);
      if (it == null || it.equals(""))
        throw new RuntimeException("Missing key: " + KEY_ITERATION);
      iteration = Integer.parseInt(it);
      if (localFiles != null && localFiles.length > 0)
        ltp = localFiles[0];
      else
        ltp = job.getTTablePath();
    }
    public void init() throws IOException {
      String pserveHost = job.get("ha.pserver.host");
      pserveHost = "localhost";
      String sp = job.get("ha.pserver.port");
      int pservePort =5444;
      if (sp != null)
        pservePort = Integer.parseInt(sp);
      useNullWord = job.includeNullWord();
      if (trainerType.equals(MODEL1_UNIFORM_INIT)) {
        trainer = new Model1_InitUniform(useNullWord);
      } else if (trainerType.equals(MODEL1_TRAINER)) {
        if (usePServer)
          ttable = new PServerClient(pserveHost, pservePort);
        else
          ttable = new TTable_monolithic_IFAs(
              ttfs, ltp, true);

        trainer = new Model1(ttable, useNullWord);
      } else if (trainerType.equals(HMM_TRAINER)) {
        if (usePServer)
          ttable = new PServerClient(pserveHost, pservePort);
        else
          ttable = new TTable_monolithic_IFAs(
              ttfs, ltp, true);
        ATable atable = loadATable(job.getATablePath(), job);
        if (!useNullWord)
          trainer = new HMM(ttable, atable);
        else
          trainer = new HMM_NullWord(ttable, atable, job.getHMMp0());
      } else
        throw new RuntimeException("Don't understand initialization stategy: " + trainerType);
    }		
  }

  public static class EMapper extends AlignmentBase
  implements Mapper<Text,PhrasePair,IntWritable,PartialCountContainer> {

    OutputCollector<IntWritable,PartialCountContainer> output_ = null;	

    public void map(Text key, PhrasePair value, 
        OutputCollector<IntWritable,PartialCountContainer> output, 
        Reporter reporter) throws IOException {

      if (output_ == null) {
        output_ = output;
        init();
        trainer.addAlignmentListener(new AEListener(reporter));
      }
      if (usePServer && ttable != null)
        ((PServerClient)ttable).query(value, useNullWord);
      AlignmentPosteriorGrid model1g= null;
      if (value.hasAlignmentPosteriors())
        model1g = value.getAlignmentPosteriorGrid();
      if (trainer instanceof HMM) {
        ((HMM)trainer).setModel1Posteriors(model1g);
      }
      trainer.processTrainingInstance(value, reporter);
      if (value.hasAlignment() && !(trainer instanceof Model1_InitUniform)) {
        PerplexityReporter pr = new PerplexityReporter();

        Alignment a = trainer.viterbiAlign(value, pr);
        a = trainer.computeAlignmentPosteriors(value).alignPosteriorThreshold(0.5f);
        ReferenceAlignment ref = (ReferenceAlignment)value.getAlignment();
        reporter.incrCounter(AlignmentEvalEnum.SURE_HITS, ref.countSureHits(a));
        reporter.incrCounter(AlignmentEvalEnum.PROBABLE_HITS, ref.countProbableHits(a));
        reporter.incrCounter(AlignmentEvalEnum.HYPOTHESIZED_ALIGNMENT_POINTS, a.countAlignmentPoints());
        reporter.incrCounter(AlignmentEvalEnum.REF_ALIGNMENT_POINTS, ref.countSureAlignmentPoints());
      }
      hasCounts = true;
    }

    public void close() {
      if (!hasCounts) return;
      try {
        trainer.clearModel();
        trainer.writePartialCounts(output_);
      } catch (IOException e) {
        throw new RuntimeException("Caught: " + e);
      }
    }
  }

  public static class AlignMapper extends AlignmentBase
  implements Mapper<Text,PhrasePair,Text,PhrasePair> {

    boolean first = true;
    Text astr = new Text();

    public void map(Text key, PhrasePair value, 
        OutputCollector<Text,PhrasePair> output, 
        Reporter reporter) throws IOException {

      if (first) {
        init();
        first = false;
        trainer.addAlignmentListener(new AEListener(reporter));
      }
      PerplexityReporter pr = new PerplexityReporter();

      AlignmentPosteriorGrid model1g= null;
      if (value.hasAlignmentPosteriors())
        model1g = value.getAlignmentPosteriorGrid();
      if (trainer instanceof HMM && model1g != null) {
        ((HMM)trainer).setModel1Posteriors(model1g);
      }

      Alignment a = trainer.viterbiAlign(value, pr);
      ReferenceAlignment ref = (ReferenceAlignment)value.getAlignment();
      AlignmentPosteriorGrid ghmm = null;
      AlignmentPosteriorGrid gmodel1 = null;

      if (generatePosteriors) {
        if (value.hasAlignmentPosteriors())
          model1g = value.getAlignmentPosteriorGrid();
        if (trainer instanceof HMM)
          ((HMM)trainer).setModel1Posteriors(model1g);
        AlignmentPosteriorGrid g = trainer.computeAlignmentPosteriors(value);
        if (value.hasAlignmentPosteriors()) {
          //System.err.println(key + ": already has posteriors!");
          model1g = value.getAlignmentPosteriorGrid();
          //model1g.penalizeGarbageCollectors(2, 0.27f, 0.20f);
          Alignment model1a = model1g.alignPosteriorThreshold(0.5f);
          //System.out.println("MODEL1 MAP ALIGNMENT:\n"+model1a.toStringVisual());
          //ystem.out.println("HMM VITERBI ALIGNMENT:\n"+a.toStringVisual());
          //model1g.diff(g);
          ghmm = g;
          gmodel1 = model1g;
          Alignment da = model1g.alignPosteriorThreshold((float)Math.exp(-1.50f));
          Alignment ints = Alignment.intersect(da, model1a);
          //Alignment df = Alignment.subtract(ints, a);
          //System.out.println("DIFF (HMM - (Model1 \\intersect DIFF)): " + key + "\n" +df.toStringVisual() + "\n"+model1g);
          //a = Alignment.union(a, df);
        }
        value.setAlignmentPosteriorGrid(g);
      }

      if (ref != null) {
        a = trainer.computeAlignmentPosteriors(value).alignPosteriorThreshold(0.5f);
        reporter.incrCounter(AlignmentEvalEnum.SURE_HITS, ref.countSureHits(a));
        reporter.incrCounter(AlignmentEvalEnum.PROBABLE_HITS, ref.countProbableHits(a));
        reporter.incrCounter(AlignmentEvalEnum.HYPOTHESIZED_ALIGNMENT_POINTS, a.countAlignmentPoints());
        reporter.incrCounter(AlignmentEvalEnum.REF_ALIGNMENT_POINTS, ref.countSureAlignmentPoints());
        if (gmodel1!=null) {
          StringBuffer sb=new StringBuffer();
          for (int i =0; i<ref.getELength(); i++)
            for (int j=0; j<ref.getFLength(); j++) {
              if (ref.isProbableAligned(j, i) || ref.isSureAligned(j, i))
                sb.append("Y");
              else
                sb.append("N");
              sb.append(" 1:").append(gmodel1.getAlignmentPointPosterior(j, i+1));
              sb.append(" 3:").append(ghmm.getAlignmentPointPosterior(j, i+1));
              if (a.aligned(j, i)) sb.append(" 4:1"); else sb.append(" 4:0");
              sb.append('\n');
            }
          //System.out.println(sb);
        }
      }
      astr.set(a.toString());
      output.collect(key, value);
    }
  }

  public static class EMReducer extends MapReduceBase
  implements Reducer<IntWritable,PartialCountContainer,IntWritable,PartialCountContainer> {
    boolean variationalBayes = false;
    IntWritable oe = new IntWritable();
    PartialCountContainer pcc = new PartialCountContainer();
    float[] counts = new float[Vocab.MAX_VOCAB_INDEX]; // TODO: fix this
    float alpha = 0.0f;
    @Override
    public void configure(JobConf job) {
      HadoopAlignConfig hac = new HadoopAlignConfig(job);
      variationalBayes = hac.useVariationalBayes();
      alpha = hac.getAlpha();
    }
    public void reduce(IntWritable key, Iterator<PartialCountContainer> values,
        OutputCollector<IntWritable,PartialCountContainer> output, 
        Reporter reporter) throws IOException {
      int lm = 0;
      if (HMM.ACOUNT_VOC_ID.get() != key.get()) {
        while (values.hasNext()) {;
        IndexedFloatArray v = (IndexedFloatArray)values.next().getContent();
        if (v.maxKey() + 1 > lm) {
          Arrays.fill(counts, lm, v.maxKey() + 1, 0.0f);
          lm = v.maxKey() + 1;
        }
        v.addTo(counts);
        }
        IndexedFloatArray sum = new IndexedFloatArray(counts, lm);
        pcc.setContent(sum);
      } else {
        ATable sum = null;
        while (values.hasNext()) {
          if (sum == null)
            sum = (ATable)((ATable)values.next().getContent()).clone();
          else 
            sum.plusEquals((ATable)values.next().getContent());
        }
        pcc.setContent(sum);
        //				pcc.normalize();
        //				if (true) throw new RuntimeException("CHECK\n"+pcc.getContent());
      }
      pcc.normalize(variationalBayes, alpha);
      output.collect(key, pcc);
    }
  }

  /**
   * Basic implementation: assume keys are IntWritable, values are Phrase
   * Better implementation: use Java Generics to templatize, ie.
   *  <Key extends WritableComparable, Value extends Writeable>
   * @author redpony
   *
   */
  public static class FileReaderZip {
    private static class SFRComp implements Comparable<SFRComp>
    {
      PartialCountContainer cur = new PartialCountContainer();
      IntWritable k = new IntWritable();
      SequenceFile.Reader s;
      boolean valid;

      public SFRComp(SequenceFile.Reader x) throws IOException {
        s = x;
        read();
      }
      public void read() throws IOException {
        valid = s.next(k, cur);
      }
      public int getKey() { return k.get(); }
      public boolean isValid() { return valid; }
      public int compareTo(SFRComp o) {
        if (!valid) throw new RuntimeException("Shouldn't happen");
        return k.get() - o.k.get();
      }
      public PartialCountContainer getValue() { return cur; }
    }

    PriorityQueue<SFRComp> pq;
    public FileReaderZip(SequenceFile.Reader[] files) throws IOException {
      pq = new PriorityQueue<SFRComp>();
      for (SequenceFile.Reader r : files) { 
        SFRComp s = new SFRComp(r);
        if (s.isValid()) pq.add(s);
      }
    }

    boolean next(IntWritable k, PartialCountContainer v) throws IOException {
      if (pq.size() == 0) return false;
      SFRComp t = pq.remove();
      v.setContent(t.getValue().getContent());
      k.set(t.getKey());
      t.read();
      if (t.isValid()) pq.add(t);
      return true;
    }
  }
  enum MergeCounters { EWORDS, STATISTICS };

  private static class ModelMergeMapper2 extends NullMapper {

    public void run(JobConf job, Reporter reporter) throws IOException {
      sLogger.setLevel(Level.INFO);

      Path outputPath = null;
      Path ttablePath = null;
      Path atablePath = null;
      HadoopAlignConfig hac = null;
      JobConf xjob = null;
      xjob = job;
      hac = new HadoopAlignConfig(job);
      ttablePath = hac.getTTablePath();
      atablePath = hac.getATablePath();
      outputPath = new Path(job.get(TTABLE_ITERATION_OUTPUT));
      IntWritable k = new IntWritable();
      PartialCountContainer t = new PartialCountContainer();
      FileSystem fileSys = FileSystem.get(xjob);
      // the following is a race condition
      fileSys.delete(outputPath.suffix("/_logs"), true);
      fileSys.delete(outputPath.suffix("/_SUCCESS"), true);
      sLogger.info("Reading from "+outputPath + ", exists? " + fileSys.exists(outputPath));
//      SequenceFile.Reader[] readers =
//        SequenceFileOutputFormat.getReaders(xjob, outputPath);
//      FileReaderZip z = new FileReaderZip(readers);
      //      while (z.next(k,t)) {
      //        if (t.getType() == PartialCountContainer.CONTENT_ARRAY) {
      //          tt.set(k.get(), (IndexedFloatArray)t.getContent());
      //          if (k.get() % 1000 == 0) reporter.progress();
      //          reporter.incrCounter(MergeCounters.EWORDS, 1);
      //          reporter.incrCounter(MergeCounters.STATISTICS, ((IndexedFloatArray)t.getContent()).size() + 1);
      //        } else {
      //          if (emittedATable)
      //            throw new RuntimeException("Should only have a single ATable!");
      //          ATable at = (ATable)t.getContent();
      //          fileSys.delete(atablePath, true);
      //          DataOutputStream dos = new DataOutputStream(
      //              new BufferedOutputStream(fileSys.create(atablePath)));
      //          at.write(dos);
      //          dos.close();
      //          emittedATable = true;
      //        }
      //      }
      TTable tt = new TTable_monolithic_IFAs(fileSys, ttablePath, false);
      boolean emittedATable = false;
      FileStatus[] status = fileSys.listStatus(outputPath);
      for (int i=0; i<status.length; i++){
        sLogger.info("Reading " + status[i].getPath() + ", exists? " + fileSys.exists(status[i].getPath()));
        SequenceFile.Reader reader = new SequenceFile.Reader(xjob, SequenceFile.Reader.file(status[i].getPath()));
        while (reader.next(k, t)){
          if (t.getType() == PartialCountContainer.CONTENT_ARRAY) {
            tt.set(k.get(), (IndexedFloatArray)t.getContent());
            if (k.get() % 1000 == 0) reporter.progress();
            reporter.incrCounter(MergeCounters.EWORDS, 1);
            reporter.incrCounter(MergeCounters.STATISTICS, ((IndexedFloatArray)t.getContent()).size() + 1);
          } else {
            if (emittedATable)
              throw new RuntimeException("Should only have a single ATable!");
            ATable at = (ATable)t.getContent();
            fileSys.delete(atablePath, true);
            DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(fileSys.create(atablePath)));
            at.write(dos);
            dos.close();
            emittedATable = true;
          }
        }
        reader.close();
      }
      fileSys.delete(ttablePath, true); // delete old ttable
      tt.write();  // write new one to same location
    }
  }


  public static class ModelMergeMapper extends MapReduceBase
  implements Mapper<LongWritable,Text,LongWritable,Text> {
    Path outputPath = null;
    Path ttablePath = null;
    Path atablePath = null;
    enum MergeCounters { EWORDS, STATISTICS };
    HadoopAlignConfig hac = null;
    JobConf xjob = null;
    public void configure(JobConf job) {
      xjob = job;
      hac = new HadoopAlignConfig(job);
      ttablePath = hac.getTTablePath();
      atablePath = hac.getATablePath();
      outputPath = new Path(job.get(TTABLE_ITERATION_OUTPUT));
    }
    public void map(LongWritable key, Text value, 
        OutputCollector<LongWritable,Text> output, 
        Reporter reporter) throws IOException {
      IntWritable k = new IntWritable();
      PartialCountContainer t = new PartialCountContainer();
      FileSystem fileSys = FileSystem.get(xjob);
      // the following is a race condition
      fileSys.delete(outputPath.suffix("/_logs"), true);
      SequenceFile.Reader[] readers =
        SequenceFileOutputFormat.getReaders(xjob, outputPath);
      FileReaderZip z = new FileReaderZip(readers);
      TTable tt = new TTable_monolithic_IFAs(fileSys, ttablePath, false);
      boolean emittedATable = false;
      while (z.next(k,t)) {
        if (t.getType() == PartialCountContainer.CONTENT_ARRAY) {
          tt.set(k.get(), (IndexedFloatArray)t.getContent());
          if (k.get() % 1000 == 0) reporter.progress();
          reporter.incrCounter(MergeCounters.EWORDS, 1);
          reporter.incrCounter(MergeCounters.STATISTICS, ((IndexedFloatArray)t.getContent()).size() + 1);
        } else {
          if (emittedATable)
            throw new RuntimeException("Should only have a single ATable!");
          ATable at = (ATable)t.getContent();
          fileSys.delete(atablePath, true);
          DataOutputStream dos = new DataOutputStream(
              new BufferedOutputStream(fileSys.create(atablePath)));
          at.write(dos);
          dos.close();
          emittedATable = true;
        }
      }
      fileSys.delete(ttablePath, true); // delete old ttable
      tt.write();  // write new one to same location
      output.collect(key, value);
    }
  }

  static double ComputeAER(Counters c) {
    double den = c.getCounter(AlignmentEvalEnum.HYPOTHESIZED_ALIGNMENT_POINTS) + c.getCounter(AlignmentEvalEnum.REF_ALIGNMENT_POINTS);
    double num = c.getCounter(AlignmentEvalEnum.PROBABLE_HITS) + c.getCounter(AlignmentEvalEnum.SURE_HITS);
    double aer = ((double)((int)((1.0 - num/den)*10000.0)))/100.0;
    double prec = ((double)((int)((((double)c.getCounter(AlignmentEvalEnum.PROBABLE_HITS)) /((double)c.getCounter(AlignmentEvalEnum.HYPOTHESIZED_ALIGNMENT_POINTS)))*10000.0)))/100.0;
    System.out.println("PREC: " + prec);
    return aer;
  }

  static final String TTABLE_ITERATION_OUTPUT = "em.model-data.file";

  static PServer pserver = null;

  static String startPServers(HadoopAlignConfig hac) throws IOException {
    int port = 4444;
    pserver = new PServer(4444, FileSystem.get(hac), hac.getTTablePath());
    Thread th = new Thread(pserver);
    th.start();
    if (true) throw new RuntimeException("Shouldn't use PServer");
    return "localhost:" + port;
  }

  static void stopPServers() throws IOException {
    if (pserver != null) pserver.stopServer();
  }

  @SuppressWarnings("deprecation")
  public static void doAlignment(int mapTasks, int reduceTasks, HadoopAlignConfig hac) throws IOException {
    System.out.println("Running alignment: " + hac);
    FileSystem fs = FileSystem.get(hac);
    Path cbtxt = new Path(hac.getRoot()+"/comp-bitext");
    //		fs.delete(cbtxt, true);
    if (!fs.exists(cbtxt)) {
      CorpusVocabNormalizerAndNumberizer.preprocessAndNumberizeFiles(hac, hac.getBitexts(), cbtxt);
    }
    System.out.println("Finished preprocessing");


    int m1iters = hac.getModel1Iterations();
    int hmmiters = hac.getHMMIterations();
    int totalIterations = m1iters + hmmiters;
    String modelType = null;
    ArrayList<Double> perps= new ArrayList<Double>();
    ArrayList<Double> aers = new ArrayList<Double>();
    boolean hmm = false;
    boolean firstHmm = true;
    Path model1PosteriorsPath = null;
    for (int iteration=0; iteration<totalIterations; iteration++) {
      long start = System.currentTimeMillis();
      hac.setBoolean("ha.generate.posterios", false);
      boolean lastIteration = (iteration == totalIterations-1);
      boolean lastModel1Iteration = (iteration == m1iters-1);
      if (iteration >= m1iters )
        hmm=true;
      if (hmm)
        modelType = "HMM";
      else
        modelType = "Model1";
      FileSystem fileSys = FileSystem.get(hac);
      String sOutputPath=modelType + ".data." + iteration;
      Path outputPath = new Path(sOutputPath);
      try {
        if (usePServer && iteration > 0) // no probs in first iteration!
          startPServers(hac);
        System.out.println("Starting iteration " + iteration + (iteration == 0 ? " (initialization)" : "") + ": " + modelType);

        JobConf conf = new JobConf(hac, HadoopAlign.class);
        conf.setJobName("EMTrain." + modelType + ".iter"+iteration);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.set(KEY_TRAINER, MODEL1_TRAINER);
        conf.set(KEY_ITERATION, Integer.toString(iteration));
        conf.set("mapred.child.java.opts", "-Xmx2048m");
        if (iteration == 0)
          conf.set(KEY_TRAINER, MODEL1_UNIFORM_INIT);
        if (hmm) {
          conf.set(KEY_TRAINER, HMM_TRAINER);
          if (firstHmm) {
            firstHmm=false;
            System.out.println("Writing default a-table...");
            Path pathATable = hac.getATablePath();
            fileSys.delete(pathATable, true);
            DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(fileSys.create(pathATable)));
            int cond_values = 1;
            if (!hac.isHMMHomogeneous()) {
              cond_values = 100;
            }
            ATable at = new ATable(hac.isHMMHomogeneous(),
                cond_values, 100); at.normalize(); at.write(dos);
                //			System.out.println(at);
                dos.close();	
          }
        }
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(PartialCountContainer.class);

        conf.setMapperClass(EMapper.class);
        conf.setReducerClass(EMReducer.class);

        conf.setNumMapTasks(mapTasks);
        conf.setNumReduceTasks(reduceTasks);
        System.out.println("Running job "+conf.getJobName());

        // if doing model1 iterations, set input to pre-processing output
        // otherwise, input is set to output of last model 1 iteration
        if (model1PosteriorsPath != null) {
          System.out.println("Input: " + model1PosteriorsPath);
          FileInputFormat.setInputPaths(conf, model1PosteriorsPath);	
        } else{
          System.out.println("Input: " + cbtxt);
          FileInputFormat.setInputPaths(conf, cbtxt);
        }

        System.out.println("Output: "+outputPath);

        FileOutputFormat.setOutputPath(conf, new Path(hac.getRoot()+"/"+outputPath.toString()));
        fileSys.delete(new Path(hac.getRoot()+"/"+outputPath.toString()), true);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        RunningJob job = JobClient.runJob(conf);
        Counters c = job.getCounters();
        double lp = c.getCounter(CrossEntropyCounters.LOGPROB);
        double wc = c.getCounter(CrossEntropyCounters.WORDCOUNT);
        double ce = lp/wc/Math.log(2);
        double perp = Math.pow(2.0, ce);
        double aer = ComputeAER(c);
        System.out.println("Iteration " + iteration + ": (" + modelType + ")\tCROSS-ENTROPY: " + ce + "   PERPLEXITY: " + perp);
        System.out.println("Iteration " + iteration + ": " + aer + " AER");
        aers.add(aer);			
        perps.add(perp);
      } finally { stopPServers(); }


      JobConf conf = new JobConf(hac, ModelMergeMapper2.class);
      System.err.println("Setting " + TTABLE_ITERATION_OUTPUT + " to " + outputPath.toString());
      conf.set(TTABLE_ITERATION_OUTPUT, hac.getRoot()+"/"+outputPath.toString());
      conf.setJobName("EMTrain.ModelMerge");
      //			conf.setOutputKeyClass(LongWritable.class);
      conf.setMapperClass(ModelMergeMapper2.class);		        
      conf.setSpeculativeExecution(false);
      conf.setNumMapTasks(1);
      conf.setNumReduceTasks(0);
      conf.setInputFormat(NullInputFormat.class);
      conf.setOutputFormat(NullOutputFormat.class);
      conf.set("mapred.map.child.java.opts", "-Xmx2048m");
      conf.set("mapred.reduce.child.java.opts", "-Xmx2048m");

      //			FileInputFormat.setInputPaths(conf, root+"/dummy");
      //			fileSys.delete(new Path(root+"/dummy.out"), true);
      //			FileOutputFormat.setOutputPath(conf, new Path(root+"/dummy.out"));
      //			conf.setOutputFormat(SequenceFileOutputFormat.class);

      System.out.println("Running job "+conf.getJobName());
      System.out.println("Input: "+hac.getRoot()+"/dummy");
      System.out.println("Output: "+hac.getRoot()+"/dummy.out");

      JobClient.runJob(conf);
      fileSys.delete(new Path(hac.getRoot()+"/"+outputPath.toString()), true);

      if (lastIteration || lastModel1Iteration) {
        //hac.setBoolean("ha.generate.posteriors", true);
        conf = new JobConf(hac, HadoopAlign.class);
        sOutputPath=modelType + ".data." + iteration;
        outputPath = new Path(sOutputPath);

        conf.setJobName(modelType + ".align");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.reduce.child.java.opts", "-Xmx2048m");

        // TODO use file cache
        /*try {
					if (hmm || iteration > 0) {
						URI ttable = new URI(fileSys.getHomeDirectory() + Path.SEPARATOR + hac.getTTablePath().toString());
						DistributedCache.addCacheFile(ttable, conf);
						System.out.println("cache<-- " + ttable);
					}

				} catch (Exception e) { throw new RuntimeException("Caught " + e); }
         */
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set(KEY_TRAINER, MODEL1_TRAINER);
        conf.set(KEY_ITERATION, Integer.toString(iteration));
        if (hmm)
          conf.set(KEY_TRAINER, HMM_TRAINER);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(PhrasePair.class);

        conf.setMapperClass(AlignMapper.class);
        conf.setReducerClass(IdentityReducer.class);

        conf.setNumMapTasks(mapTasks);
        conf.setNumReduceTasks(reduceTasks);
        FileOutputFormat.setOutputPath(conf, new Path(hac.getRoot()+"/"+outputPath.toString()));

        //if last model1 iteration, save output path, to be used as input path in later iterations
        if (lastModel1Iteration) {
          FileInputFormat.setInputPaths(conf, cbtxt);
          model1PosteriorsPath = new Path(hac.getRoot()+"/"+outputPath.toString());
        } else {
          FileInputFormat.setInputPaths(conf, model1PosteriorsPath);					
        }

        fileSys.delete(outputPath, true);

        System.out.println("Running job "+conf.getJobName());

        RunningJob job = JobClient.runJob(conf);
        System.out.println("GENERATED: " + model1PosteriorsPath);
        Counters c = job.getCounters();
        double aer = ComputeAER(c);
        //				System.out.println("Iteration " + iteration + ": (" + modelType + ")\tCROSS-ENTROPY: " + ce + "   PERPLEXITY: " + perp);
        System.out.println("Iteration " + iteration + ": " + aer + " AER");
        aers.add(aer);			
        perps.add(0.0);
      }

      long end = System.currentTimeMillis();
      System.out.println(modelType + " iteration " + iteration + " took " + ((end - start) / 1000) + " seconds.");

    }
    for (int i = 0; i < perps.size(); i++) {
      System.out.print("I="+i+"\t");
      if (aers.size() > 0) {
        System.out.print(aers.get(i)+"\t");
      }
      System.out.println(perps.get(i));
    }
  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( HadoopAlign.class.getCanonicalName(), options );
  }

  private static final String INPUT_OPTION = "input";
  private static final String WORK_OPTION = "workdir";
  private static final String FLANG_OPTION = "src_lang";
  private static final String ELANG_OPTION = "trg_lang";
  private static final String MODEL1_OPTION = "model1";
  private static final String HMM_OPTION = "hmm";
  private static final String REDUCE_OPTION = "reduce";
  private static final String TRUNCATE_OPTION = "use_truncate";
  private static final String LIBJARS_OPTION = "libjars";

  private static Options options;

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to XML-formatted parallel corpus").withArgName("path").hasArg().isRequired().create(INPUT_OPTION));
    options.addOption(OptionBuilder.withDescription("path to work/output directory on HDFS").withArgName("path").hasArg().isRequired().create(WORK_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter collection language code").withArgName("en|de|fr|zh|es|ar|tr").hasArg().isRequired().create(FLANG_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter collection language code").withArgName("en|de|fr|zh|es|ar|tr").hasArg().isRequired().create(ELANG_OPTION));
    options.addOption(OptionBuilder.withDescription("number of IBM Model 1 iterations").withArgName("positive integer").hasArg().create(MODEL1_OPTION));
    options.addOption(OptionBuilder.withDescription("number of HMM iterations").withArgName("positive integer").hasArg().create(HMM_OPTION));
    options.addOption(OptionBuilder.withDescription("truncate/stem text or not").create(TRUNCATE_OPTION));
    options.addOption(OptionBuilder.withDescription("number of reducers").withArgName("positive integer").hasArg().create(REDUCE_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      printUsage();
      System.err.println("Error parsing command line: " + exp.getMessage());
      return;
    }

    String bitextPath = cmdline.getOptionValue(INPUT_OPTION);
    String workDir = cmdline.getOptionValue(WORK_OPTION);
    String srcLang = cmdline.getOptionValue(FLANG_OPTION);
    String trgLang = cmdline.getOptionValue(ELANG_OPTION);

    int model1Iters = cmdline.hasOption(MODEL1_OPTION) ? Integer.parseInt(cmdline.getOptionValue(MODEL1_OPTION)) : 0;
    int hmmIters = cmdline.hasOption(HMM_OPTION) ? Integer.parseInt(cmdline.getOptionValue(HMM_OPTION)) : 0;
    if (model1Iters + hmmIters == 0) {
      System.err.println("Please enter a positive number of iterations for either Model 1 or HMM");
      printUsage();
      return;
    }
    boolean isTruncate = cmdline.hasOption(TRUNCATE_OPTION) ? true : false;
    int numReducers = cmdline.hasOption(REDUCE_OPTION) ? Integer.parseInt(cmdline.getOptionValue(REDUCE_OPTION)) : 50;

    HadoopAlignConfig hac = new HadoopAlignConfig(workDir,
        trgLang, srcLang,
        bitextPath,
        model1Iters,
        hmmIters,
        true, 	// use null word
        false, 	// use variational bayes
        isTruncate, 	// use word truncation
        0.00f  	// alpha
    );
    hac.setHMMHomogeneous(false);
    hac.set("mapreduce.map.memory.mb", "2048");
    hac.set("mapreduce.map.java.opts", "-Xmx2048m");
    hac.set("mapreduce.reduce.memory.mb", "2048");
    hac.set("mapreduce.reduce.java.opts", "-Xmx2048m");
    hac.setHMMp0(0.2);
    hac.setMaxSentLen(15);

    doAlignment(50, numReducers, hac);
  }

}
