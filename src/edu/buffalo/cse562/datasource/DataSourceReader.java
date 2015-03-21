package edu.buffalo.cse562.datasource;

import java.io.IOException;

import net.sf.jsqlparser.expression.LeafValue;

public interface DataSourceReader {

	public LeafValue[] readNextTuple() throws IOException ;
	
	public void close() throws IOException;
}
