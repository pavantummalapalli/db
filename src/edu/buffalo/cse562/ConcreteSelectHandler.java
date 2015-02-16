package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class ConcreteSelectHandler implements SelectHandler<Void> {

	public ConcreteSelectHandler() {
	}

	@Override
	public Void processStatement(Select selectStatement) {
		//Implement the select statement handler.
		CreateTable sg;
		ColumnDefinition df = new ColumnDefinition();
		ExampleVisitor object = new ExampleVisitor();
		selectStatement.getSelectBody().accept(object);
		return null;
	}
}
