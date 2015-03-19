package edu.buffalo.cse562.queryplan;

import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public abstract class AbstractJoinNode implements Operator {

	private Node parentNode;
	private Node relationNode1;
	private Node relationNode2;
	private Expression expression;
	private Set<String> tableNames;
	
	public void setTableNames(Set<String> tableNames) {
		this.tableNames = tableNames;
	}
	
	public Set<String> getTableNames() {
		return tableNames;
	}
	
	public void setRelationNode1(Node RelationNode1) {
		this.relationNode1 = RelationNode1;
	}
	public Node getRelationNode1() {
		return relationNode1;
	}
	public void setRelationNode2(Node RelationNode2) {
		this.relationNode2 = RelationNode2;
	}
	public Node getRelationNode2() {
		return relationNode2;
	}
	
	@Override
	public Node getParentNode() {
		return parentNode;
	}
	@Override
	public void setParentNode(Node parentNode) {
		this.parentNode = parentNode;
	}
	@Override
	public void addJoinCondition(Expression exp) {
		if(expression!=null){
			Expression exp1 = new AndExpression(expression, exp);
			expression=exp1;
		}
		else
			expression=exp;
	}
	@Override
	public Expression getJoinCondition() {
		return expression;
	}
}
