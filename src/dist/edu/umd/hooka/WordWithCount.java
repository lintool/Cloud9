package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import edu.umd.hooka.IntAugmentedPhrase;

import org.apache.hadoop.io.Writable;

public class WordWithCount implements Writable {

	public int word;
	public int count;
	
	public WordWithCount()
	{
		word = 0;
		count = 0;
	}
	
	public WordWithCount(int w, int c)
	{
		word = w;
		count = c;
	}
	
	public static WordWithCount read(DataInput input) throws IOException {
		WordWithCount newItem = new WordWithCount();
		newItem.word = input.readInt();
		newItem.count = input.readInt();
		return newItem;
	}
	
	public void readFields(DataInput input) throws IOException {
		word = input.readInt();
		count = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(word);
		output.writeInt(count);
	}
	
	public IntAugmentedPhrase getIntAugmentedPhrase()
	{
		int[] x = {word};
		int[] y = {count};
		return new IntAugmentedPhrase(x, 0, y);
	}

}
