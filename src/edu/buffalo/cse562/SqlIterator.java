package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.LeafValue;

public interface SqlIterator {
	public LeafValue[] next();
	public String[] nextAggregate();
	public void close();
}
