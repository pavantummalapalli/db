package edu.buffalo.cse562.datasource;

import java.io.IOException;

import net.sf.jsqlparser.expression.LeafValue;

public interface DataSourceWriter {

	
	public void writeNextTuple(LeafValue[] tuple) throws IOException ;
	
	public void close() throws IOException;
}
