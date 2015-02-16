package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

public class ExampleVisitor implements SelectVisitor {

	public ExampleVisitor() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void visit(PlainSelect arg0) {
		//process Plain Select
		
	}

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub

	}
}
