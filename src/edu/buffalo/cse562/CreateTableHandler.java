package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public interface CreateTableHandler<T> {

	public T processStatement(CreateTable createTableStatement);
	
}
