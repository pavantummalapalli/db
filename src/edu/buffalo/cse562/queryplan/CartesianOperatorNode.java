package edu.buffalo.cse562.queryplan;

import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

public class CartesianOperatorNode implements Node{
	
	private EqualsTo expression;
	private Node relationNode1;
	private Node relationNode2;
	
	public void setExpression(EqualsTo expression) {
		this.expression = expression;
	}
	public EqualsTo getExpression() {
		return expression;
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
	public RelationNode eval(){
		//TODO
		return null;
	}
}