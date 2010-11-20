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

public class PairOfObjectLong<L> {

	private L mLeftElement;
	private long mRightElement;

	public PairOfObjectLong(L left, long right) {
		mLeftElement = left;
		mRightElement = right;
	}

	public PairOfObjectLong() {}

	public L getLeftElement() {
		return mLeftElement;
	}

	public long getRightElement() {
		return mRightElement;
	}

	public void set(L left, long right) {
		mLeftElement = left;
		mRightElement = right;
	}

	public void setLeftElement(L left) {
		mLeftElement = left;
	}

	public void setRightElement(long right) {
		mRightElement = right;
	}
}
