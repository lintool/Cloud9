package edu.umd.hooka.alignment;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public final class EFMarginals {

	IndexedFloatArray fmarginals = new IndexedFloatArray();
	IndexedFloatArray emarginals = new IndexedFloatArray();
	
	public EFMarginals(FileSystem fs, Path p) throws IOException {
		Path emp = p.suffix(Path.SEPARATOR + "part-00000");
		Path fmp = p.suffix(Path.SEPARATOR + "part-00001");
		FSDataInputStream in = fs.open(emp);
		emarginals.readFields(in); in.close();
		in = fs.open(fmp);
		fmarginals.readFields(in); in.close();
	}

	float getETotal(int eword) {
		return emarginals.get(eword);
	}

	float getFTotal(int fword) {
		return fmarginals.get(fword);
	}
}
