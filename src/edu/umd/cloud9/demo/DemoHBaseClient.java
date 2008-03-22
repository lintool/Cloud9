package edu.umd.cloud9.demo;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.io.Text;

public class DemoHBaseClient {

	public static void main(String[] args) throws IOException {

		// This is a continuation of the HBase Shell primer---read that first!

		// open the table
		HBaseConfiguration conf = new HBaseConfiguration();
		HTable table = new HTable(conf, new Text("test"));

		// illustration of adding data
		System.out.println("Illustration of adding data...");

		// column we're going to write
		Text col = new Text("family1:col1");
		Text rowid = new Text();

		// iterate over rows
		for (int i = 0; i < 10; i++) {
			rowid.set(new Integer(i).toString());
			long writeid = table.startUpdate(rowid);
			Text val = new Text("test" + (i * i));

			// values get stored as byte[]
			table.put(writeid, col, val.getBytes());
			table.commit(writeid);

			System.out.println("Writing row = " + rowid + ", col '" + col + "' = " + val);
		}

		// illustration of querying
		System.out.println("\nIllustration of querying...");

		String s = Text.decode(table.get(new Text("1"), new Text("family1:col1")));
		System.out.println("row = 1, 'family1:col1' = " + s);

		// illustration of scanning
		System.out.println("\nIllustration of scanning...");

		// we only want one column, but you can specify multiple columns to
		// fetch at once
		Text[] cols = { col };

		// initialize the scanner
		HScannerInterface scanner = table.obtainScanner(cols, new Text());

		// column values are stored in a Map
		SortedMap<Text, byte[]> values = new TreeMap<Text, byte[]>();
		HStoreKey currentKey = new HStoreKey();
		while (scanner.next(currentKey, values)) {
			// decode the stored byte[] back into a String
			String val = Text.decode(values.get(col));
			System.out.println("row = " + currentKey + ", col '" + col + "' = " + val);
		}

		// remember to close scanner when done
		scanner.close();

	}

}
