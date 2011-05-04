package edu.umd.hooka;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CLI_Int2Words {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			System.err.println("Usage: CLI_Int2Words <vocfile.dat>");
			System.exit(1);
		}
		try {
			Path pve = new Path(args[0]);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			FileSystem fileSys = FileSystem.get(conf);

			DataInputStream dis = new DataInputStream(new BufferedInputStream(fileSys.open(pve)));
			VocabularyWritable v = new VocabularyWritable();
			v.readFields(dis);
			BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
			String l = null;
			while((l = r.readLine()) != null) {
				String[] nums=l.split("\\s+");
				//System.err.println("words: " + nums.length);
				for (String n: nums) {
					if (n.length() == 0) continue;
					System.out.print(v.get(Integer.parseInt(n)));
					System.out.print(' ');
				}
				System.out.println();
			}
		} catch (IOException e) {
			System.err.println("Caught: " + e);
			System.exit(1);
		}
	}

}
