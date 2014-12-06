package edu.umd.cloud9.debug;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WritableComparatorTestHarness {

  @SuppressWarnings("rawtypes")
	public static int compare(WritableComparator comparator, WritableComparable obj1,
			 WritableComparable obj2) {

		byte[] bytes1 = null, bytes2 = null;

		try {
			ByteArrayOutputStream bytesOut1 = new ByteArrayOutputStream();
			DataOutputStream dataOut1 = new DataOutputStream(bytesOut1);
			obj1.write(dataOut1);
			bytes1 = bytesOut1.toByteArray();

			ByteArrayOutputStream bytesOut2 = new ByteArrayOutputStream();
			DataOutputStream dataOut2 = new DataOutputStream(bytesOut2);
			obj2.write(dataOut2);
			bytes2 = bytesOut2.toByteArray();

		} catch (IOException e) {
			e.printStackTrace();
		}

		return comparator.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length);
	}
}
