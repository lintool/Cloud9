package edu.umd.cloud9.collection.wt10g;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKI;

/**
 * <p>
 * Object that maps between DOCNOs (String identifiers) to docnos
 * (sequentially-numbered ints). This object provides mappings for the wt10G
 * collection; the docnos are numbered all the way through whole collection.
 * </p>
 * 
 * @author Fangyue Wang
 */
public class Wt10GDocnoMapping implements DocnoMapping
{
	/**
	 * Creates a {@code Wt10GDocnoMapping} object
	 */
	public Wt10GDocnoMapping()
	{
	}

	@Override
	public int getDocno(String docid)
	{
		if (docid == null)
			return -1;

		int sec;
		int sub;
		int doc;

		try
		{
			sec = Integer.parseInt(docid.substring(3, 6));
			sub = Integer.parseInt(docid.substring(8, 10));
			doc = Integer.parseInt(docid.substring(11));
		} catch (NumberFormatException e)
		{
			return -1;
		}
		return sec*100000+sub*1000+doc;
	}

	@Override
	public String getDocid(int docno)
	{
		int docn = docno % 100000000;
		int sec = docn / 100000;
		docn%=100000;
		int sub = docn / 1000;
		docn%=1000;
		int doc = docn;
		
		return String.format("WTX%03d-B%02d-%d", sec, sub, doc);
	}

	@Override
	public void loadMapping(Path p, FileSystem fs) throws IOException
	{
		// I assume that Wt10g docno can be clearly derived from docid
		// So dont need mapping file
		// Fangyue Wang

		// FSLineReader reader = new FSLineReader(p, fs);
		// Text t = new Text();
		// int cnt = 0;
		// String prevSec = null;
		//
		// while (reader.readLine(t) > 0)
		// {
		// String[] arr = t.toString().split(",");
		//
		// if (prevSec == null || !arr[0].equals(prevSec))
		// {
		// subdirMapping.put(arr[0], cnt);
		// }
		//
		// offets[cnt] = Integer.parseInt(arr[3]);
		// prevSec = arr[0];
		// cnt++;
		// }
		//
		// reader.close();
	}
}