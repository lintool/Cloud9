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

package edu.umd.cloud9.webgraph.data;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Nima Asadi
 * 
 */

public class AnchorTextTarget implements WritableComparable<AnchorTextTarget>,
    Iterable<Integer> {

  private ArrayListOfIntsWritable sources;
  private int target;
  private float weight;

  public AnchorTextTarget() {
    sources = new ArrayListOfIntsWritable();
  }

  public AnchorTextTarget(AnchorTextTarget at) {
    this();
    target = at.target;
    weight = at.weight;
    addSources(at.sources);
  }

  public AnchorTextTarget(int trgt, ArrayListOfIntsWritable srcs, float wt) {
    this();
    target = trgt;
    weight = wt;
    addSources(srcs);
  }

  public void readFields(DataInput in) throws IOException {
    sources.clear();
    sources.readFields(in);
    target = in.readInt();
    weight = in.readFloat();
  }

  public void write(DataOutput out) throws IOException {
    sources.write(out);
    out.writeInt(target);
    out.writeFloat(weight);
  }

  public void addSources(ArrayListOfIntsWritable sources) {
    for (int i : sources)
      this.sources.add(i);
  }

  public void setSources(ArrayListOfIntsWritable sources) {
    this.sources = sources;
  }

  public ArrayListOfIntsWritable getSources() {
    return sources;
  }

  public void setTarget(int target) {
    this.target = target;
  }

  public int getTarget() {
    return target;
  }

  public void setWeight(float wt) {
    this.weight = wt;
  }

  public float getWeight() {
    return weight;
  }

  public int compareTo(AnchorTextTarget at) {
    if (weight < at.weight)
      return 1;
    else if (weight > at.weight)
      return -1;
    else if (target < at.target)
      return 1;
    else if (target > at.target)
      return -1;

    return 0;
  }

  public boolean equals(Object o) {
    AnchorTextTarget other = (AnchorTextTarget) o;

    if (other.target != this.target)
      return false;

    return this.weight == other.weight;
  }

  public int hashCode() {
    return target;
  }

  public String toString() {
    return "[ to=" + getTarget() + ", from=" + getSources() + ", weight="
        + getWeight() + " ]";
  }

  public Iterator<Integer> iterator() {
    return sources.iterator();
  }

}
