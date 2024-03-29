package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.LeafValue;

public class BufferedIterator implements SqlIterator {

	private SqlIterator iterator;
	
	public BufferedIterator(SqlIterator iterator) {
		this.iterator=iterator;
	}
	
	@Override
	public LeafValue[] next() {
		return iterator.next();
	}

	@Override
	public void nextAggregate() {
		iterator.nextAggregate();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
