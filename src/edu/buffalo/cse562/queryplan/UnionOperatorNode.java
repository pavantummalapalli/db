package edu.buffalo.cse562.queryplan;

import java.util.List;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class UnionOperatorNode implements Operator {

	private List<Node> childNodes;

	public void setChildNodes(List<Node> childNodes) {
		this.childNodes = childNodes;
	}

	public List<Node> getChildNodes() {
		return childNodes;
	}

	@Override
	public RelationNode eval() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CreateTable evalSchema() {
		// TODO Auto-generated method stub
		return null;
	}
}
