package edu.buffalo.cse562.queryplan;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public interface DataSource {

	public Reader getReader() throws IOException ;
	
	public Writer getWriter() throws IOException ;
	
	public void close() throws IOException;
	
}
