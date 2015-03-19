package edu.buffalo.cse562.queryplan;

import net.sf.jsqlparser.expression.Expression;

public interface Operator extends Node{

	public Expression getJoinCondition();
	public void addJoinCondition(Expression exp);
	
}
