package edu.buffalo.cse562.fileoperations.sort;

import java.util.List;

public interface Merger<V> {

	public List<V> mergeData(V object1,V object2);
	
	public V writeMergedData(V object1);
}
