package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class VocabularyWritable implements Writable, Vocab {
	private static final Logger sLogger = Logger.getLogger(VocabularyWritable.class);

	ArrayList<String> strings;
	HashMap<String, Integer> map;
	public VocabularyWritable()
	{
		strings = new ArrayList<String>();
		strings.add("NULL");
		map = new HashMap<String, Integer>();
		map.put("NULL", new Integer(0));
	}
	
	public int size() {
		return strings.size();
	}
	
	public int addOrGet(String word)
	{
		Integer i = map.get(word);
		if (i == null) {
			i = new Integer(strings.size());
			strings.add(word);
			map.put(word, i);
		}
		return i.intValue();
	}
	
	public int get(String word) {
		if(map.get(word)==null){
			return -1;
		}else{
			return map.get(word).intValue();
		}
	}
	
	public String get(int index) {
		return strings.get(index);
	}
	
	public void readFields(DataInput in) throws IOException {
		int s = in.readInt();
		sLogger.info("VOCAB SIZE "+s);
		strings = new ArrayList<String>(s);
		map = new HashMap<String, Integer>();
		Text t = new Text();
		for (int i=0; i<s; i++) {
			t.readFields(in);
			String str = t.toString();
			strings.add(i, str);
			map.put(str, new Integer(i));
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(strings.size());
		Text t= new Text();
		for (String s: strings) {
			t.set(s);
			t.write(out);
		}
	}
	
	public String toString(){
		return strings.toString();
	}

}
