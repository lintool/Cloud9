package edu.umd.cloud9.util;

import java.util.List;

public interface SortableEntries<T extends Comparable<T>> extends Iterable<T> {
  public static enum Order {
    ByLeftElementAscending, ByLeftElementDescending, ByRightElementAscending, ByRightElementDescending
  };

  public List<T> getEntries(Order ordering);
  public List<T> getEntries(Order ordering, int n);
}
