package edu.buffalo.cse562.datasource;

import java.io.IOException;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;

public class DataSourceReaderBDB implements DataSourceReader {
	
	public DataSourceReaderBDB(Expression expression) {
		
	}
	
	@Override
	public LeafValue[] readNextTuple() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}
}
