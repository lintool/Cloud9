package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import edu.umd.cloud9.util.ArrayListOfInts;


public class ArrayListOfIntsWritable extends ArrayListOfInts implements Writable {
	
	public ArrayListOfIntsWritable() {
		super();
	}

	public ArrayListOfIntsWritable(int firstNumber, int lastNumber) {
		super();
		for(int i=firstNumber;i<lastNumber;i++){
			this.add(i, i);
		}	
	}
	
	public ArrayListOfIntsWritable(int initialCapacity) {
		super(initialCapacity);
	}
	
	public ArrayListOfIntsWritable(ArrayListOfIntsWritable domain) {
		super();
		for(int i=0;i<domain.size();i++){
			add(i, domain.get(i));
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.clear();
		int size = in.readInt();
		for(int i=0;i<size;i++){
			add(i,in.readInt());
		}
	}

	public void write(DataOutput out) throws IOException {
		int size = size();
		out.writeInt(size);
		for(int i=0;i<size;i++){
			out.writeInt(get(i));
		}
	}
	
	public String toString(){
		if(this==null){
			return "null";
		}
		int size = size();
		if(size==0){
			return "[]";
		}
		
		String s="[";
		for(int i=0;i<size-1;i++){
			s+=get(i)+",";
		}
		s+=get(size-1)+"]";
		return s;
	}

	public ArrayListOfIntsWritable intersection(ArrayListOfIntsWritable domain) {
		ArrayListOfIntsWritable intDomain = new ArrayListOfIntsWritable();
		int len, curPos=0;
		if(size()<domain.size()){
			len=size();
			for(int i=0;i<len;i++){
				int elt=this.get(i);
				while(curPos<domain.size() && domain.get(curPos)<elt){
					curPos++;
				}
				if(curPos>=domain.size()){
					return intDomain;
				}else if(domain.get(curPos)==elt){
					intDomain.add(elt);
				}
			}
		}else{
			len=domain.size();
			for(int i=0;i<len;i++){
				int elt=domain.get(i);
				while(curPos<size() && get(curPos)<elt){
					curPos++;
				}
				if(curPos>=size()){
					return intDomain;
				}else if(get(curPos)==elt){
					intDomain.add(elt);
				}
			}
		}
		if(intDomain.size()==0){
			intDomain=null;
		}
		return intDomain;
	}

}
