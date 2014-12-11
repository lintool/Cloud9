package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlignmentPosteriorGrid {

	Array2D posteriors;
	int elen = 0;
	int flen = 0;
	
	public void write(DataOutput out) throws IOException {
		throw new IOException("foo");
	}

	public void readFiles(DataInput in) throws IOException {
		throw new IOException("foo");
	}
	
	public AlignmentPosteriorGrid(PhrasePair pp) {
		elen = pp.getE().getWords().length + 1; // room for NULL
		flen = pp.getF().getWords().length;
		
		posteriors = new Array2D(elen * flen);
		posteriors.resize(flen, elen);
	}
	
	public float getAlignmentPointPosterior(int f, int e) {
		return posteriors.get(f, e);
	}
	
	public void setAlignmentPointPosterior(int f, int e, float p) {
		posteriors.set(f, e, p);
	}
	
	public Alignment alignPosteriorThreshold(float t) {
		Alignment res = new Alignment(flen, elen-1);
		for (int i =1; i<elen; ++i) {
			for (int j=0; j<flen; ++j) {
				if (getAlignmentPointPosterior(j, i) > t)
					res.align(j, i-1);
			}
		}
		return res;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < posteriors.getSize2(); i++) {
			for (int j = 0; j < posteriors.getSize1(); j++) {
				float p = posteriors.get(j, i);
				if (p > 0.0f) {
					double lp = Math.log(p);
					int c = 10000;
					if (lp <= -10.0) c /= 10;
					if (lp <= -100.0) c /= 10;
					if (lp <= -1000.0) c /= 10;
					if (lp <= -10000.0) c /= 10;
					int ip = (int)(lp * c);
			
					float llp = ((float)ip)/(float)c;
					sb.append(llp);
				} else { sb.append("-inf"); }
				sb.append('\t');
			}
			sb.append("\n");
		}
		return sb.toString();
	}
}
