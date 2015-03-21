package edu.buffalo.cse562.datasource;

import java.io.IOException;

public interface DataSource {

	public DataSourceReader getReader() throws IOException;
	public DataSourceWriter getWriter() throws IOException;
	public void clear();
}
