package edu.umd.cloud9.debug;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

public class InMemoryOutputCollector implements OutputCollector<WritableComparable, Writable> {

	private SortedMap<WritableComparable, List<Writable>> mValues;

	public InMemoryOutputCollector() {
		mValues = new TreeMap<WritableComparable, List<Writable>>();
	}

	private Writable clone(Writable w) {
		Writable obj = null;

		try {
			obj = w.getClass().newInstance();

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			w.write(dataOut);

			obj.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		} catch (Exception e) {
			e.printStackTrace();
		}

		return obj;
	}

	public void collect(WritableComparable key, Writable value) {

		// must clone the key and value, since it may be reused on the caller
		WritableComparable k = (WritableComparable) clone(key);
		Writable v = clone(value);

		if (mValues.containsKey(k)) {
			mValues.get(k).add(v);
		} else {
			List<Writable> list = new ArrayList<Writable>();
			list.add(v);
			mValues.put(k, list);
		}

	}

	public Iterator<Writable> getValues(Writable key) {
		if (!mValues.containsKey(key))
			return null;

		return mValues.get(key).iterator();
	}

	public Iterator<WritableComparable> getUniqueKeys() {
		return mValues.keySet().iterator();
	}

	public void printAll() {
		Iterator<WritableComparable> keyIter = getUniqueKeys();
		while (keyIter.hasNext()) {
			WritableComparable key = keyIter.next();
			System.out.print(key + ": ");

			Iterator<Writable> valueIter = getValues(key);
			while (valueIter.hasNext()) {
				Writable value = valueIter.next();

				System.out.print(value + (valueIter.hasNext() ? ", " : ""));
			}

			System.out.print("\n");
		}
	}
}
