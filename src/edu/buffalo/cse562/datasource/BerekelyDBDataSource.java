package edu.buffalo.cse562.datasource;

import java.io.IOException;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.queryplan.RelationNode;

public class BerekelyDBDataSource implements DataSource,DataSourceReader{

	
	public BerekelyDBDataSource(RelationNode node) {
		Expression exp =node.getExpression();
		
	}
	
	
	@Override
	public DataSourceReader getReader() throws IOException {
		return this;
	}

	@Override
	public DataSourceWriter getWriter() throws IOException {
		return null;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
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
