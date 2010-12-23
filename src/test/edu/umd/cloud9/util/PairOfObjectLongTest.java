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

import java.util.Collections;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.common.collect.Lists;

public class PairOfObjectLongTest {

	@Test
	public void test1() {
		PairOfObjectLong<String> pair = new PairOfObjectLong<String>("a", 1L);
		assertEquals("a", pair.getLeftElement());
		assertEquals(1L, pair.getRightElement());

		pair.setLeftElement("b");
		assertEquals("b", pair.getLeftElement());

		pair.setRightElement(5L);
		assertEquals(5L, pair.getRightElement());

		pair.set("foo", -1L);
		assertEquals("foo", pair.getLeftElement());
		assertEquals(-1L, pair.getRightElement());
	}

	@Test
	public void testIterable() {
		List<PairOfObjectLong<String>> list = Lists.newArrayList();

		list.add(new PairOfObjectLong<String>("f", 9L));
		list.add(new PairOfObjectLong<String>("a", 1L));
		list.add(new PairOfObjectLong<String>("b", 4L));
		list.add(new PairOfObjectLong<String>("b", 2L));
		list.add(new PairOfObjectLong<String>("c", 2L));
		list.add(new PairOfObjectLong<String>("a", 3L));

		assertEquals(6, list.size());
		Collections.sort(list);

		assertEquals("a", list.get(0).getLeftElement());
		assertEquals(1, list.get(0).getRightElement());
		assertEquals("a", list.get(1).getLeftElement());
		assertEquals(3, list.get(1).getRightElement());
		assertEquals("b", list.get(2).getLeftElement());
		assertEquals(2, list.get(2).getRightElement());
		assertEquals("b", list.get(3).getLeftElement());
		assertEquals(4, list.get(3).getRightElement());
		assertEquals("c", list.get(4).getLeftElement());
		assertEquals(2, list.get(4).getRightElement());
		assertEquals("f", list.get(5).getLeftElement());
		assertEquals(9, list.get(5).getRightElement());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfObjectLongTest.class);
	}
}
