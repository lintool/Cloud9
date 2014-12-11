package edu.umd.hooka;

import java.util.Arrays;

import edu.umd.hooka.alignment.ZeroProbabilityException;

public final class Array2D {
	float[] data;
	int width;
	int size;
	public Array2D(int maxCap) {
		data = new float[maxCap];
	}
	public void resize(int x, int y) {
		int s = x * y;
		if (s > data.length)
			throw new RuntimeException("Requested size larger than allocated space: x="+x +" y="+y);
		size = s;
		width = x;
		this.fill(0.0f);
	}
	public void fill(float val) {
		Arrays.fill(data, 0, size, val);
	}
	public float get(int x, int y) {
		return data[y * width + x];
	}
	public void set(int x, int y, float v) {
		data[y * width + x] = v;
	}
	public int getSize1() {
		return width;
	}
	public int getSize2() {
		return size / width;
	}
	public float normalizeColumn(int c) {
		float sum = 0.0f;
		int cur = c;
		int r = getSize2();
		for (int i = 0; i < r; i++) {
			sum += data[cur];
			cur += width;
		}
		if (sum == 0.0f) {
			StringBuffer sb = new StringBuffer();
			sb.append("normalizeColumn(").append(c).append("):");
			cur = c;
			for (int i = 0; i < r; i++) {
				sb.append(' ').append(data[cur]);
				cur += width;
			}
			sb.append(" sum=0.0");
			throw new ZeroProbabilityException(sb.toString());
		}
		cur = c;
		for (int i = 0; i < r; i++) {
			data[cur] /= sum;
			cur += width;
		}
		return sum;
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
