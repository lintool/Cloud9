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

package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class HistogramTest {

	@Test
	public void test1() {
		Histogram<String> h = new Histogram<String>();

		h.count("yes");
		h.count("yes");
		h.count("yes");
		h.count("no");
		h.count("no");
		h.count("maybe");
		h.count("so");

		assertEquals(3, h.getCount("yes"));
		assertEquals(2, h.getCount("no"));
		assertEquals(1, h.getCount("maybe"));
		assertEquals(1, h.getCount("so"));

		Iterator<Map.Entry<String, Integer>> iter = h.getSortedEntries().iterator();
		Map.Entry<String, Integer> e = null;

		e = iter.next();
		assertEquals("yes", e.getKey());
		assertEquals(3, e.getValue().intValue());

		e = iter.next();
		assertEquals("no", e.getKey());
		assertEquals(2, e.getValue().intValue());

		e = iter.next();
		assertEquals("maybe", e.getKey());
		assertEquals(1, e.getValue().intValue());

		e = iter.next();
		assertEquals("so", e.getKey());
		assertEquals(1, e.getValue().intValue());

		assertEquals(7, h.getTotalCount());

		h.clear();

		assertEquals(0, h.getTotalCount());

		assertEquals(0, h.getCount("yes"));
		assertEquals(0, h.getCount("no"));
		assertEquals(0, h.getCount("maybe"));
		assertEquals(0, h.getCount("so"));

		h.count("yes");
		h.count("yes");
		h.count("no");

		assertEquals(2, h.getCount("yes"));
		assertEquals(1, h.getCount("no"));

	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HistogramTest.class);
	}

}
