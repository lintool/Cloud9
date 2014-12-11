package edu.umd.cloud9.debug;

public class MemoryUsageUtils {

	public static long getUsedMemory() {
		return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
	}
	
	public static long getUsedMemoryAccurate() {
		gc();
		long totalMemory = Runtime.getRuntime().totalMemory();
		gc();
		long freeMemory = Runtime.getRuntime().freeMemory();
		long usedMemory = totalMemory - freeMemory;
		return usedMemory;
	}

	public static void gc() {
		try {
			System.gc();
			Thread.sleep(100);
			System.runFinalization();
			Thread.sleep(100);
			System.gc();
			Thread.sleep(100);
			System.runFinalization();
			Thread.sleep(100);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
