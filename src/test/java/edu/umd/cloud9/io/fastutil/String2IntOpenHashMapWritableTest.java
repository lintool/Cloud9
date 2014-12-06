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

package edu.umd.cloud9.io.fastutil;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.map.String2IntOpenHashMapWritable;

public class String2IntOpenHashMapWritableTest {

  @Test
  public void testBasic() throws IOException {
    String2IntOpenHashMapWritable m = new String2IntOpenHashMapWritable();

    m.put("hi", 5);
    m.put("there", 22);

    assertEquals(2, m.size());
    assertEquals(5, m.getInt("hi"));

    m.remove("hi");
    assertEquals(1, m.size());

    assertEquals(22, m.getInt("there"));
  }

  @Test
  public void testAccent() throws IOException {
    String2IntOpenHashMapWritable map1 = new String2IntOpenHashMapWritable();

    // '\u00E0': [LATIN SMALL LETTER A WITH GRAVE]
    // '\u00E6': [LATIN SMALL LETTER AE]
    // '\u00E7': [LATIN SMALL LETTER C WITH CEDILLA]
    // '\u00FC': [LATIN SMALL LETTER U WITH DIAERESIS]

    map1.put("\u00E0", 1);
    map1.put("\u00E6", 2);
    map1.put("\u00E7", 3);
    map1.put("\u00FC", 4);

    assertEquals(1, map1.getInt("\u00E0"));
    assertEquals(2, map1.getInt("\u00E6"));
    assertEquals(3, map1.getInt("\u00E7"));
    assertEquals(4, map1.getInt("\u00FC"));

    map1.increment("\u00E0");
    map1.increment("\u00E6");
    map1.increment("\u00E7");
    map1.increment("\u00FC");

    assertEquals(2, map1.getInt("\u00E0"));
    assertEquals(3, map1.getInt("\u00E6"));
    assertEquals(4, map1.getInt("\u00E7"));
    assertEquals(5, map1.getInt("\u00FC"));

    map1.put("\u00E0", 10);
    map1.remove("\u00E6");
    map1.remove("\u00E7");
    map1.put("\u00E7", 2);
    map1.increment("\u00FC");

    assertEquals(10, map1.getInt("\u00E0"));
    assertEquals(2, map1.getInt("\u00E7"));
    assertEquals(6, map1.getInt("\u00FC"));

    assertEquals(3, map1.size());

    // Test serialization
    String2IntOpenHashMapWritable map2 = String2IntOpenHashMapWritable.create(map1.serialize());
    assertEquals(10, map2.getInt("\u00E0"));
    assertEquals(2, map2.getInt("\u00E7"));
    assertEquals(6, map2.getInt("\u00FC"));
  }

  @Test
  public void testJp() throws IOException {
    String2IntOpenHashMapWritable map1 = new String2IntOpenHashMapWritable();
    BufferedReader in = new BufferedReader(new InputStreamReader(
        this.getClass().getClassLoader().getResourceAsStream("jp-sample.txt"), "UTF8"));

    List<String> list = Lists.newArrayList();
    int cnt = 0;
    String line;
    while ((line = in.readLine()) != null) {
      list.add(line);
      map1.put(line, cnt++);
    }

    for (int i = 0; i < list.size(); i++) {
      assertEquals(i, map1.getInt(list.get(i)));
    }
    assertEquals(5, map1.size());

    for (int i = 0; i < list.size(); i++) {
      map1.increment(list.get(i));
    }
    assertEquals(5, map1.size());

    for (int i = 0; i < list.size(); i++) {
      assertEquals(i + 1, map1.getInt(list.get(i)));
    }
    assertEquals(5, map1.size());

    // Test serialization
    String2IntOpenHashMapWritable map2 = String2IntOpenHashMapWritable.create(map1.serialize());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(i + 1, map2.getInt(list.get(i)));
    }
    assertEquals(5, map2.size());

    for (int i = 0; i < list.size(); i++) {
      map2.remove(list.get(i));
    }
    assertEquals(5, map1.size());
    assertEquals(0, map2.size());

    in.close();
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(String2IntOpenHashMapWritableTest.class);
  }
}
