package edu.umd.cloud9.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class ExceptionUtils {
	public static String getStackTrace(Exception e) {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		PrintStream stream = new PrintStream(bytes);
		e.printStackTrace(stream);

		return new String(bytes.toByteArray());
	}
}
