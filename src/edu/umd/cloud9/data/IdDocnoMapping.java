package edu.umd.cloud9.data;

import java.io.IOException;

public interface IdDocnoMapping {
	public int getDocno(String docid);

	public void loadMapping(String f) throws IOException;
}
