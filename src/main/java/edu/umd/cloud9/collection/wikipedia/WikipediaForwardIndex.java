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
import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Forward index for Wikipedia collections.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikipediaForwardIndex implements DocumentForwardIndex<WikipediaPage> {
  private static final Logger LOG = Logger.getLogger(WikipediaPage.class);

  private Configuration conf;

  private int[] docnos;
  private int[] offsets;
  private short[] fileno;
  private String collectionPath;
  private int lastDocno = -1;

  private WikipediaDocnoMapping docnoMapping = new WikipediaDocnoMapping();

  public WikipediaForwardIndex() {
    conf = new Configuration();
  }

  public WikipediaForwardIndex(Configuration conf) {
    this.conf = Preconditions.checkNotNull(conf);
  }

  @Override
  public void loadIndex(Path index, Path mapping, FileSystem fs) throws IOException {
    LOG.info("Loading forward index: " + index);
    docnoMapping.loadMapping(mapping, fs);

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

    if (idx < 0) {
      idx = -idx - 2;
    }

    try {
      FileSystem fs = FileSystem.get(conf);
      DecimalFormat df = new DecimalFormat("00000");
      Path file = new Path(collectionPath + "/part-m-" + df.format(fileno[idx]));
      // Try the old file naming convention.
      if (!fs.exists(file)) {
        file = new Path(collectionPath + "/part-" + df.format(fileno[idx]));
      }

      LOG.info("fetching docno " + docno + ": seeking to " + offsets[idx] + " at " + file);

      SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(file));

      IntWritable key = new IntWritable();
      WikipediaPage value = WikipediaPageFactory.createWikipediaPage(conf.get("wiki.language"));

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
    return getDocument(docnoMapping.getDocno(docid));
  }

  @Override
  public int getDocno(String docid) {
    return docnoMapping.getDocno(docid);
  }

  @Override
  public String getDocid(int docno) {
    return docnoMapping.getDocid(docno);
  }

  @Override
  public int getFirstDocno() {
    return docnos[0];
  }

  @Override
  public int getLastDocno() {
    if (lastDocno != -1) {
      return lastDocno;
    }

    // find the last entry, and then see all the way to the end of the
    // collection
    int idx = docnos.length - 1;

    try {
      FileSystem fs = FileSystem.get(conf);
      DecimalFormat df = new DecimalFormat("00000");
      Path file = new Path(collectionPath + "/part-m-" + df.format(fileno[idx]));
      // Try the old file naming convention.
      if (!fs.exists(file)) {
        file = new Path(collectionPath + "/part-" + df.format(fileno[idx]));
      }

      SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(file));
      IntWritable key = new IntWritable();

      reader.seek(offsets[idx]);

      while (reader.next(key));
      lastDocno = key.get();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return lastDocno;
  }
}
