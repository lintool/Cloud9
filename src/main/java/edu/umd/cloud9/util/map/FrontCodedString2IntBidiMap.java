package edu.umd.cloud9.util.map;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class FrontCodedString2IntBidiMap {
  private static final Logger LOG = Logger.getLogger(FrontCodedString2IntBidiMap.class);

  private FrontCodedStringList stringList;
  private ShiftAddXorSignedStringMap stringHash;
  private IntArrayList intList = new IntArrayList();
  private Int2IntOpenHashMap int2PosMap = new Int2IntOpenHashMap();

  public FrontCodedString2IntBidiMap(FileSystem fs, Path path) throws IOException {
    FSDataInputStream in = fs.open(path);

    byte[] bytes;
    ObjectInputStream obj;

    bytes = new byte[in.readInt()];
    LOG.info("Loading front-coded list of terms: " + bytes.length + " bytes.");
    in.readFully(bytes);
    obj = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      stringList = (FrontCodedStringList) obj.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    obj.close();

    bytes = new byte[in.readInt()];
    LOG.info("Loading dictionary hash: " + bytes.length + " bytes.");
    in.readFully(bytes);
    obj = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      stringHash = (ShiftAddXorSignedStringMap) obj.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    obj.close();

    int n = in.readInt();
    for (int i = 0; i < n; i++) {
      int id = in.readInt();
      intList.add(id);
      int2PosMap.put(id, i);
    }
    LOG.info("Finished loading.");

    in.close();
  }

  public int get(String s) {
    return intList.getInt((int) stringHash.getLong(s));
  }

  public String get(int i) {
    return stringList.get(int2PosMap.get(i)).toString();
  }
}
