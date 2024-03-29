package edu.buffalo.cse562.datasource;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;

public class BufferDataSource implements DataSource,DataSourceReader,DataSourceWriter {
	
	private List<LeafValue[]> tuples = new LinkedList<LeafValue[]>();
	private Iterator<LeafValue[]> iterator = tuples.iterator();

	@Override
	public LeafValue[] readNextTuple() throws IOException {
		if(iterator.hasNext())
			return iterator.next();
		return null;
	}

	@Override
	public void writeNextTuple(LeafValue[] tuple) throws IOException {
		tuples.add(tuple);
	}

	@Override
	public void close(){
		
	}

	@Override
	public DataSourceReader getReader() throws IOException{
		close();
		iterator = tuples.iterator();
		return this;
	}

	@Override
	public DataSourceWriter getWriter() throws IOException{
		close();
		clear();
		return this;
	}

	@Override
	public void clear() {
		tuples=new LinkedList<LeafValue[]>();
	}

	@Override
	public long getEstimatedDataSourceSize() {
		return 200 * 1024 * 1024;
	}
}
