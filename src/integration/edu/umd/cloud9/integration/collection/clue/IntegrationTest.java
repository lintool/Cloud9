package edu.umd.cloud9.integration.collection.clue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.io.*;
import java.util.List;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcDocnoMappingBuilder;
import edu.umd.cloud9.collection.clue.ClueWarcForwardIndex;
import edu.umd.cloud9.collection.clue.ClueWarcForwardIndexBuilder;
import edu.umd.cloud9.collection.clue.CountClueWarcRecords;
import edu.umd.cloud9.integration.IntegrationUtils;

public class IntegrationTest {
  private static final Random random = new Random();

  private static final Path collectionPath =
      new Path("/shared/collections/ClueWeb09/collection.compressed.block/en.01");
  private static final String tmpPrefix = "tmp-" + IntegrationTest.class.getCanonicalName() +
      "-" + random.nextInt(10000);

  private static final String mappingFile = tmpPrefix + "-mapping.dat";

    //  @Test
  public void testDocnoMapping() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    ClueWarcDocnoMappingBuilder.main(conf, new String[] { libjars,
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + collectionPath,
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + mappingFile });

    ClueWarcDocnoMapping mapping = new ClueWarcDocnoMapping();
    mapping.loadMapping(new Path(mappingFile), fs);

    assertEquals("clueweb09-en0000-00-00000", mapping.getDocid(1));
    assertEquals("clueweb09-en0000-29-13313", mapping.getDocid(1000000));

    assertEquals(1, mapping.getDocno("clueweb09-en0000-00-00000"));
    assertEquals(1000000, mapping.getDocno("clueweb09-en0000-29-13313"));
  }

  @Test
  public void testDemoCountDocs() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
  
    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    /*
    CountClueWarcRecords.main(conf, new String[] { libjars,
        "-repacked",
        "-path=" + collectionPath,
        "-docnoMapping=" + mappingFile });
    */

String cmd = "hadoop jar dist/cloud9-1.3.6.jar edu.umd.cloud9.collection.clue.CountClueWarcRecords " +
    "-libjars=/fs/clip-hadoop/jimmylin/Cloud9/dist/cloud9-1.3.6.jar,/fs/clip-hadoop/jimmylin/Cloud9/lib/guava-13.0.1.jar " +
   "-repacked -path=/shared/collections/ClueWeb09/collection.compressed.block/en.01 " +
    "-docnoMapping=/shared/collections/ClueWeb09/docno-mapping.dat ";

    Runtime rt = Runtime.getRuntime();
    System.out.println("Execing " + cmd);
    Process proc = rt.exec(cmd);
    // any error message?
            StreamGobbler errorGobbler = new 
                StreamGobbler(proc.getErrorStream(), "ERROR");            
            
            // any output?
            StreamGobbler outputGobbler = new 
                StreamGobbler(proc.getInputStream(), "OUTPUT");
                
            // kick them off
            errorGobbler.start();
            outputGobbler.start();
                                    
            // any error???
            int exitVal = proc.waitFor();
            System.out.println("ExitValue: " + exitVal);

  }
  
    //@Test
  public void testForwardIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String index = tmpPrefix + "-findex.dat";
    ClueWarcForwardIndexBuilder.main(conf, new String[] { libjars,
        "-collection=" + collectionPath,
        "-index=" + index });

    ClueWarcForwardIndex findex = new ClueWarcForwardIndex();
    findex.loadIndex(new Path(index), new Path(mappingFile), fs);

    assertTrue(findex.getDocument(14069750).getContent()
        .contains("Vizergy: How Design and SEO work together"));
    assertTrue(findex.getDocument("clueweb09-en0008-76-19728").getContent()
        .contains("Jostens - Homeschool Yearbooks"));
    assertEquals(1, findex.getFirstDocno());
    assertEquals(50220423, findex.getLastDocno());
  }
  
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegrationTest.class);
  }
}

class StreamGobbler extends Thread
{
    InputStream is;
    String type;
    
    StreamGobbler(InputStream is, String type)
    {
        this.is = is;
        this.type = type;
    }
    
    public void run()
    {
        try
	    {
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line=null;
		while ( (line = br.readLine()) != null)
		    System.out.println(type + ">" + line);    
            } catch (IOException ioe)
	    {
                ioe.printStackTrace();  
	    }
    }
}
