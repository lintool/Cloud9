/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.io.map;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class JpEncodingTest {

  private void writeOutput(String str, String file) {
    try {
      FileOutputStream fos = new FileOutputStream(file);
      Writer out = new OutputStreamWriter(fos, "UTF8");
      out.write(str);
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String readInput(String file) {
    StringBuffer buffer = new StringBuffer();
    try {
      FileInputStream fis = new FileInputStream(file);
      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      Reader in = new BufferedReader(isr);
      int ch;
      while ((ch = in.read()) > -1) {
        buffer.append((char) ch);
      }
      in.close();
      return buffer.toString();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Test
  public void testJp() throws IOException {
    // See:
    //  - http://hints.macworld.com/article.php?story=20050208053951714
    //  - http://download.oracle.com/javase/tutorial/i18n/text/stream.html
    PrintStream out = new PrintStream(System.out, true, "UTF-8");
    String jpString = new String("\u65e5\u672c\u8a9e\u6587\u5b57\u5217");

    String file = "test.txt";
    writeOutput(jpString, file);
    String inputString = readInput(file);

    out.println(jpString + " " + inputString);
    assertEquals(jpString, inputString);
    new File(file).delete();
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(JpEncodingTest.class);
  }
}
