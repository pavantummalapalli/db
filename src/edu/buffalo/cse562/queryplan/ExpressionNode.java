package edu.buffalo.cse562.queryplan;

import net.sf.jsqlparser.expression.Expression;

public class ExpressionNode implements Node {

	private Expression expression;
	private Node evaluatedNode;
	private Node childNode;
	
	public ExpressionNode(Expression expression) {
		this.expression=expression;
	}
	
	@Override
	public RelationNode eval() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}
	public Node getChildNode() {
		return childNode;
	}
}
