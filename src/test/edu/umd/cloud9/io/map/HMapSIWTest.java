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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.common.collect.Lists;

public class HMapSIWTest {

  @Test
  public void testBasic() throws IOException {
    HMapSIW m = new HMapSIW();

    m.put("hi", 5);
    m.put("there", 22);

    assertEquals(2, m.size());
    assertEquals(5, m.get("hi"));

    m.remove("hi");
    assertEquals(1, m.size());

    assertEquals(22, m.get("there"));
  }

  @Test
  public void testAccent() throws IOException {
    HMapSIW map1 = new HMapSIW();

    // '\u00E0': à [LATIN SMALL LETTER A WITH GRAVE]
    // '\u00E6': æ [LATIN SMALL LETTER AE]
    // '\u00E7': ç [LATIN SMALL LETTER C WITH CEDILLA]
    // '\u00FC': ü [LATIN SMALL LETTER U WITH DIAERESIS]

    map1.put("à", 1);
    map1.put("æ", 2);
    map1.put("ç", 3);
    map1.put("ü", 4);

    assertEquals(1, map1.get("à"));
    assertEquals(2, map1.get("æ"));
    assertEquals(3, map1.get("ç"));
    assertEquals(4, map1.get("ü"));

    assertEquals(1, map1.get("\u00E0"));
    assertEquals(2, map1.get("\u00E6"));
    assertEquals(3, map1.get("\u00E7"));
    assertEquals(4, map1.get("\u00FC"));

    map1.increment("à");
    map1.increment("æ");
    map1.increment("ç");
    map1.increment("ü");

    assertEquals(2, map1.get("à"));
    assertEquals(3, map1.get("æ"));
    assertEquals(4, map1.get("ç"));
    assertEquals(5, map1.get("ü"));

    assertEquals(2, map1.get("\u00E0"));
    assertEquals(3, map1.get("\u00E6"));
    assertEquals(4, map1.get("\u00E7"));
    assertEquals(5, map1.get("\u00FC"));

    map1.put("à", 10);
    map1.remove("æ");
    map1.remove("ç");
    map1.put("ç", 2);
    map1.increment("ü");

    assertEquals(10, map1.get("à"));
    assertEquals(2, map1.get("ç"));
    assertEquals(6, map1.get("ü"));

    assertEquals(3, map1.size());

    // Test serialization
    HMapSIW map2 = HMapSIW.create(map1.serialize());
    assertEquals(10, map2.get("à"));
    assertEquals(2, map2.get("ç"));
    assertEquals(6, map2.get("ü"));
  }

  @Test
  public void testJp() throws IOException {
    HMapSIW map1 = new HMapSIW();
    BufferedReader in = new BufferedReader(new InputStreamReader(
        new FileInputStream("etc/jp-sample.txt"), "UTF8"));

    List<String> list = Lists.newArrayList();
    int cnt = 0;
    String line;
    while ((line = in.readLine()) != null) {
      list.add(line);
      map1.put(line, cnt++);
    }

    for (int i = 0; i < list.size(); i++) {
      assertEquals(i, map1.get(list.get(i)));
    }
    assertEquals(5, map1.size());

    for (int i = 0; i < list.size(); i++) {
      map1.increment(list.get(i));
    }
    assertEquals(5, map1.size());

    for (int i = 0; i < list.size(); i++) {
      assertEquals(i + 1, map1.get(list.get(i)));
    }
    assertEquals(5, map1.size());

    // Test serialization
    HMapSIW map2 = HMapSIW.create(map1.serialize());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(i + 1, map2.get(list.get(i)));
    }
    assertEquals(5, map2.size());

    for (int i = 0; i < list.size(); i++) {
      map2.remove(list.get(i));
    }
    assertEquals(5, map1.size());
    assertEquals(0, map2.size());
  }

  @Test
  public void testSerialize1() throws IOException {
    HMapSIW m1 = new HMapSIW();

    m1.put("hi", 5);
    m1.put("there", 22);

    HMapSIW n2 = HMapSIW.create(m1.serialize());

    String key;
    float value;

    assertEquals(n2.size(), 2);

    key = "hi";
    value = n2.get(key);
    assertTrue(value == 5);

    value = n2.remove(key);
    assertEquals(n2.size(), 1);

    key = "there";
    value = n2.get(key);
    assertTrue(value == 22);
  }

  @Test
  public void testSerializeEmpty() throws IOException {
    HMapSIW m1 = new HMapSIW();

    assertTrue(m1.size() == 0);

    HMapSIW m2 = HMapSIW.create(m1.serialize());

    assertTrue(m2.size() == 0);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(HMapSIWTest.class);
  }
}
