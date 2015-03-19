package edu.buffalo.cse562.fileoperations.sort;

public interface Convertor<V> {

	public V parseFromString(String data);
	
	public String convertToString(V data);
	
}
