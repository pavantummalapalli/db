package edu.buffalo.cse562.queryplan;

import java.util.List;

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
}
