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

package edu.umd.cloud9.io.pair;

public class Pair {
	public static PairOfFloatInt of(float left, int right) {
		return new PairOfFloatInt(left, right);
	}

	public static PairOfFloats of(float left, float right) {
		return new PairOfFloats(left, right);
	}

	public static PairOfIntLong of(int left, long right) {
		return new PairOfIntLong(left, right);
	}

	public static PairOfInts of(int left, int right) {
		return new PairOfInts(left, right);
	}

	public static PairOfIntString of(int left, String right) {
		return new PairOfIntString(left, right);
	}

	public static PairOfLongFloat of(long left, float right) {
		return new PairOfLongFloat(left, right);
	}

	public static PairOfLongInt of(long left, int right) {
		return new PairOfLongInt(left, right);
	}

	public static PairOfLongs of(long left, long right) {
		return new PairOfLongs(left, right);
	}

	public static PairOfStringFloat of(String left, float right) {
		return new PairOfStringFloat(left, right);
	}

	public static PairOfStringLong of(String left, long right) {
		return new PairOfStringLong(left, right);
	}

	public static PairOfStringInt of(String left, int right) {
		return new PairOfStringInt(left, right);
	}

	public static PairOfStrings of(String left, String right) {
		return new PairOfStrings(left, right);
	}
}
