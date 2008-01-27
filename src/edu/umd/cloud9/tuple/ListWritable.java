package edu.umd.cloud9.tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class ListWritable<E extends Writable> implements Writable {

	private List<E> mList;

	public ListWritable() {
		mList = new ArrayList<E>();
	}

	public E get(int index) {
		return mList.get(index);
	}

	public void add(E element) {
		mList.add(element);
	}

	public void set(int index, E element) {
		mList.set(index, element);
	}

	public int size() {
		return mList.size();
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		int numFields = in.readInt();

		for (int i = 0; i < numFields; i++) {
			try {
				String className = in.readUTF();

				int sz = in.readInt();
				byte[] bytes = new byte[sz];
				in.readFully(bytes);

				E obj = (E) Class.forName(className).newInstance();
				obj.readFields(new DataInputStream(new ByteArrayInputStream(
						bytes)));
				this.add(obj);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(mList.size());

		for (int i = 0; i < mList.size(); i++) {
			if (mList.get(i) == null) {
				throw new RuntimeException("Cannot serialize null fields!");
			}

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			mList.get(i).write(dataOut);

			out.writeUTF(mList.get(i).getClass().getCanonicalName());
			out.writeInt(bytesOut.size());
			out.write(bytesOut.toByteArray());
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for ( int i=0; i<this.size(); i++) {
			if ( i != 0 )
				sb.append(", ");
			sb.append(this.get(i));
		}
		sb.append("]");
		
		return sb.toString();
	}
}
