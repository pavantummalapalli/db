package edu.buffalo.cse562.queryplan;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public interface Node {
	public RelationNode eval();
	public CreateTable evalSchema();
}
