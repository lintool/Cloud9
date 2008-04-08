package edu.umd.cloud9.tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;


import org.apache.hadoop.io.Writable;

public class HashMapWritable<K extends Writable, V extends Writable> extends HashMap<K, V> implements
        Writable {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
	 * Creates a HashMapWritable object.
	 */
	public HashMapWritable() {
		super();
	}
	
	/**
	 * Creates a HashMapWritable object from a regular HashMap.
	 */
	public HashMapWritable(HashMap<K, V> map) {
		super(map);
	}

	/**
	 * Deserializes the array.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		
		this.clear();

		int numEntries = in.readInt();
		if(numEntries==0) return;
		
		String keyClassName = in.readUTF();
		String valueClassName = in.readUTF();
		
		K objK;
		V objV;
		try {
			Class keyClass = Class.forName(keyClassName);
			Class valueClass = Class.forName(valueClassName);
			for (int i = 0; i < numEntries; i++) {
				objK = (K) keyClass.newInstance();
				objK.readFields(in);
				objV = (V) valueClass.newInstance();
				objV.readFields(in);
				put(objK, objV);
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
	 * Serializes this array.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		// Write out the number of entries in the map
	    out.writeInt(size());
	    if(size()==0) return;
	    
	    // Write out the class names for keys and values
	    // assuming that data is homogeneuos (i.e., all entries have same types)
	    Set<Map.Entry<K, V>> entries = entrySet();
	    Map.Entry<K, V> first = entries.iterator().next();
	    K objK = first.getKey();
	    V objV = first.getValue();
	    out.writeUTF(objK.getClass().getCanonicalName());
	    out.writeUTF(objV.getClass().getCanonicalName());

	    // Then write out each key/value pair
	    for (Map.Entry<K, V> e: entrySet()) {
	      e.getKey().write(out);
	      e.getValue().write(out);
	    }
	}

}
