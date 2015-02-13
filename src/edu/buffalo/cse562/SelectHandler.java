package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.select.Select;

public interface SelectHandler<T> {
	
	public T processStatement(Select selectStatement);
	
}
