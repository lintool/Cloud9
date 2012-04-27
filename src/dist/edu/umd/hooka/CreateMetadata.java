package edu.umd.hooka;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class CreateMetadata {
	
	public static void GenerateMetadata(Path bitextPath, Path resultPath) throws IOException
	{
		System.out.println(bitextPath.toString());
		JobConf conf = new JobConf(CreateMetadata.class);
		FileSystem fileSys = FileSystem.get(conf);
		//SequenceFile.Reader[] x = SequenceFileOutputFormat.getReaders(conf, bitextPath);
		SequenceFile.Reader[] x = SequenceFileOutputFormat.getReaders(conf, new Path("/shared/bitexts/ar-en.ldc.10k/ar-en.10k.bitext"));
		WritableComparable key = new IntWritable();
		PhrasePair value = new PhrasePair();
		int sc = 0;
		int ec = 0;
		int fc = 0;
		try{
			for(SequenceFile.Reader r : x)
				while(r.next(key, value))
				{
					sc = sc + 1;
					for(int word: value.getE().getWords())
						if(word > ec) ec=word;
					for(int word: value.getF().getWords())
						if(word > fc) fc=word;
				}
			}
		catch(IOException e){throw new RuntimeException("IO exception: " + e.getMessage());}
		Metadata theMetadata = new Metadata(sc, ec, fc);
		ObjectOutputStream mdstream = new ObjectOutputStream(new BufferedOutputStream(FileSystem.get(conf).create(resultPath)));
		mdstream.writeObject(theMetadata);
		mdstream.close();
		
}
}
