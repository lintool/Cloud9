package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.collection.DocumentForwardIndex;

/**
 * Forward index for Wikipedia collections.
 *
 * @author Jimmy Lin
 */
public class WikipediaForwardIndex implements DocumentForwardIndex<WikipediaPage> {
  private static final Logger LOG = Logger.getLogger(WikipediaPage.class);

  private Configuration conf;
  private FileSystem fs;

  private int[] docnos;
  private int[] offsets;
  private short[] fileno;
  private String collectionPath;

  private WikipediaDocnoMapping mDocnoMapping = new WikipediaDocnoMapping();

  public WikipediaForwardIndex() {
    conf = new Configuration();
  }

  public WikipediaForwardIndex(Configuration conf) {
    this.conf = Preconditions.checkNotNull(conf);
  }

  @Override
  public void loadIndex(Path index, Path mapping, FileSystem fs) throws IOException {
    LOG.info("Loading forward index: " + index);
    mDocnoMapping.loadMapping(mapping, fs);

    FSDataInputStream in = fs.open(index);

    // Class name; throw away.
    in.readUTF();
    collectionPath = in.readUTF();

    int blocks = in.readInt();

    LOG.info(blocks + " blocks expected");
    docnos = new int[blocks];
    offsets = new int[blocks];
    fileno = new short[blocks];

    for (int i = 0; i < blocks; i++) {
      docnos[i] = in.readInt();
      offsets[i] = in.readInt();
      fileno[i] = in.readShort();

      if (i > 0 && i % 100000 == 0)
        LOG.info(i + " blocks read");
    }

    in.close();
  }

  @Override
  public String getCollectionPath() {
    return collectionPath;
  }

  @Override
  public WikipediaPage getDocument(int docno) {
    long start = System.currentTimeMillis();

    // trap invalid docnos
    if (docno < getFirstDocno() || docno > getLastDocno())
      return null;

    int idx = Arrays.binarySearch(docnos, docno);

    if (idx < 0)
      idx = -idx - 2;

    DecimalFormat df = new DecimalFormat("00000");
    String file = collectionPath + "/part-" + df.format(fileno[idx]);

    LOG.info("fetching docno " + docno + ": seeking to " + offsets[idx] + " at " + file);

    try {

      SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file), conf);

      IntWritable key = new IntWritable();
      WikipediaPage value = new WikipediaPage();

      reader.seek(offsets[idx]);

      while (reader.next(key)) {
        if (key.get() == docno)
          break;
      }
      reader.getCurrentValue(value);
      reader.close();
      long duration = System.currentTimeMillis() - start;

      LOG.info(" docno " + docno + " fetched in " + duration + "ms");
      return value;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public WikipediaPage getDocument(String docid) {
    return getDocument(mDocnoMapping.getDocno(docid));
  }

  @Override
  public int getDocno(String docid) {
    return mDocnoMapping.getDocno(docid);
  }

  @Override
  public String getDocid(int docno) {
    return mDocnoMapping.getDocid(docno);
  }

  @Override
  public int getFirstDocno() {
    return docnos[0];
  }

  private int mLastDocno = -1;

  @Override
  public int getLastDocno() {
    if (mLastDocno != -1) {
      return mLastDocno;
    }

    // find the last entry, and then see all the way to the end of the
    // collection
    int idx = docnos.length - 1;

    DecimalFormat df = new DecimalFormat("00000");
    String file = collectionPath + "/part-" + df.format(fileno[idx]);

    try {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file), conf);
      IntWritable key = new IntWritable();

      reader.seek(offsets[idx]);

      while (reader.next(key))
        ;
      mLastDocno = key.get();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return mLastDocno;
  }
}
