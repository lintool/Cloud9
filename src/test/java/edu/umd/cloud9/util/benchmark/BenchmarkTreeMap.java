package edu.umd.cloud9.util.benchmark;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.google.common.collect.Lists;

import edu.umd.cloud9.debug.MemoryUsageUtils;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.MapIV;
import edu.umd.cloud9.util.map.TMapIV;
import edu.umd.cloud9.util.map.MapIV.Entry;

public class BenchmarkTreeMap {
  private static final int NUM_TRIALS = 50000;
  private static final int NUM_FEATURES = 300;

  private static final boolean SAVE = false;
  private static final boolean SUM = true;
  private static final int KEY_RANGE = 1000000;
  private static final int VALUE_RANGE = 1000;

  private static final Random rand = new Random();

  public static void main(String[] args) {
    benchmarkTreeMap();
    benchmarkTMapIV();
    benchmarkHMapIV();

    benchmarkHMapIV();
    benchmarkTMapIV();
    benchmarkTreeMap();
  }

  private static void benchmarkTreeMap() {
    long startTime = System.currentTimeMillis();
    long usedMemory1 = MemoryUsageUtils.getUsedMemory();

    List<TreeMap<Integer, String>> lst = Lists.newArrayList();
    for (int i = 0; i < NUM_TRIALS; i++) {
      TreeMap<Integer, String> map = new TreeMap<Integer, String>();
      while (map.size() < NUM_FEATURES) {
        map.put(rand.nextInt(KEY_RANGE), rand.nextInt(VALUE_RANGE) + "");
      }

      if (SUM) {
        int sum = 0;
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
          sum += entry.getKey();
        }
      }
      if (SAVE) {
        lst.add(map);
      }
    }

    System.out.println(System.currentTimeMillis() - startTime);
    System.out.println(MemoryUsageUtils.getUsedMemory() - usedMemory1);
  }

  private static void benchmarkTMapIV() {
    long startTime = System.currentTimeMillis();
    long usedMemory1 = MemoryUsageUtils.getUsedMemory();

    List<TMapIV<String>> lst = Lists.newArrayList();
    for (int i = 0; i < NUM_TRIALS; i++) {
      TMapIV<String> map = new TMapIV<String>();
      while (map.size() < NUM_FEATURES) {
        map.put(rand.nextInt(KEY_RANGE), rand.nextInt(VALUE_RANGE) + "");
      }

      if (SUM) {
        int sum = 0;
        for (MapIV.Entry<String> entry : map.entrySet()) {
          sum += entry.getKey();
        }
      }
      if (SAVE) {
        lst.add(map);
      }
    }

    System.out.println(System.currentTimeMillis() - startTime);
    System.out.println(MemoryUsageUtils.getUsedMemory() - usedMemory1);
  }

  private static void benchmarkHMapIV() {
    long startTime = System.currentTimeMillis();
    long usedMemory1 = MemoryUsageUtils.getUsedMemory();

    List<HMapIV<String>> lst = Lists.newArrayList();

    for (int i = 0; i < NUM_TRIALS; i++) {
      HMapIV<String> map = new HMapIV<String>();
      while (map.size() < NUM_FEATURES) {
        map.put(rand.nextInt(KEY_RANGE), rand.nextInt(VALUE_RANGE) + "");
      }

      List<MapIV.Entry<String>> entries = Lists.newArrayList(map.entrySet());
      Collections.sort(entries, new Comparator<MapIV.Entry<String>>() {
        @Override
        public int compare(Entry<String> o1, Entry<String> o2) {
          // Should never have duplicate keys.
          return o1.getKey() > o2.getKey() ? 1 : -1;
        }
      });

      if (SUM) {
        int sum = 0;
        for (MapIV.Entry<String> entry : entries) {
          sum += entry.getKey();
        }
      }
      if (SAVE) {
        lst.add(map);
      }
    }

    System.out.println(System.currentTimeMillis() - startTime);
    System.out.println(MemoryUsageUtils.getUsedMemory() - usedMemory1);
  }
}
