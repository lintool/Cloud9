package edu.umd.cloud9.io.map;

import it.unimi.dsi.fastutil.objects.Object2FloatMap;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class Object2FloatOpenHashMapWritable<K extends Writable>
    extends Object2FloatOpenHashMap<K> implements Writable {
  private static final long serialVersionUID = 902668143762455L;

  /**
   * Creates a <code>String2IntOpenHashMapWritable</code> object.
   */
  public Object2FloatOpenHashMapWritable() {
    super();
  }

  /**
   * Deserializes the map.
   *
   * @param in source for raw byte representation
   */
  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    this.clear();

    int numEntries = in.readInt();
    if (numEntries == 0)
      return;

    String keyClassName = in.readUTF();

    K objK;

    try {
      Class<K> keyClass = (Class<K>) Class.forName(keyClassName);
      for (int i = 0; i < numEntries; i++) {
        objK = (K) keyClass.newInstance();
        objK.readFields(in);
        float s = in.readFloat();
        put(objK, s);
      }

    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
  }

  /**
   * Serializes the map.
   * 
   * @param out where to write the raw byte representation
   */
  public void write(DataOutput out) throws IOException {
    // Write out the number of entries in the map
    out.writeInt(size());
    if (size() == 0)
      return;

    // Write out the class names for keys and values
    // assuming that data is homogeneous (i.e., all entries have same types)
    Set<Object2FloatMap.Entry<K>> entries = object2FloatEntrySet();
    Object2FloatMap.Entry<K> first = entries.iterator().next();
    K objK = first.getKey();
    out.writeUTF(objK.getClass().getCanonicalName());

    // Then write out each key/value pair
    for (Object2FloatMap.Entry<K> e : object2FloatEntrySet()) {
      e.getKey().write(out);
      out.writeFloat(e.getValue());
    }
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   * 
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from serialized representation.
   *
   * @param in source of serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static <K extends Writable> Object2FloatOpenHashMapWritable<K> create(DataInput in)
      throws IOException {
    Object2FloatOpenHashMapWritable<K> m = new Object2FloatOpenHashMapWritable<K>();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from serialized representation.
   *
   * @param bytes source of serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static <K extends Writable> Object2FloatOpenHashMapWritable<K> create(byte[] bytes)
      throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  /**
   * Adds values of keys from another map to this map.
   * 
   * @param m the other map
   */
  public void plus(Object2FloatOpenHashMapWritable<K> m) {
    for (Object2FloatMap.Entry<K> e : m.object2FloatEntrySet()) {
      K key = e.getKey();

      if (this.containsKey(key)) {
        this.put(key, this.get(key) + e.getValue());
      } else {
        this.put(key, e.getValue());
      }
    }
  }

  /**
   * Computes the dot product of this map with another map.
   * 
   * @param m the other map
   */
  public int dot(Object2FloatOpenHashMapWritable<K> m) {
    int s = 0;

    for (Object2FloatMap.Entry<K> e : m.object2FloatEntrySet()) {
      K key = e.getKey();

      if (this.containsKey(key)) {
        s += this.get(key) * e.getValue();
      }
    }

    return s;
  }

  /**
   * Increments the key. If the key does not exist in the map, its value is set to one.
   * 
   * @param key key to increment
   */
  public void increment(K key) {
    if (this.containsKey(key)) {
      this.put(key, this.get(key) + 1);
    } else {
      this.put(key, 1);
    }
  }
}
