package edu.umd.hooka.alignment.hmm;

import java.util.Arrays;

public final class IntArray2D {
	int[] data;
	int width;
	int size;
	public IntArray2D(int maxCap) {
		data = new int[maxCap];
	}
	public void resize(int x, int y) {
		int s = x * y;
		if (s > data.length)
			throw new RuntimeException("Requested size larger than allocated space");
		size = s;
		width = x;
		this.fill(0);
	}
	public void fill(int val) {
		Arrays.fill(data, 0, size, val);
	}
	public int get(int x, int y) {
		return data[y * width + x];
	}
	public void set(int x, int y, int v) {
		data[y * width + x] = v;
	}
	public int getSize1() {
		return width;
	}
	public int getSize2() {
		return size / width;
	}
	public String toString() {
		StringBuffer sb = new StringBuffer();
		int r = getSize2();
		int c = getSize1();
		for (int j = 0; j < r; j++) {
			for (int i = 0; i < c; i++) {
				sb.append("  ").append(get(i,j));
			}
			sb.append('\n');
		}
		return sb.toString();
	}
}
