package edu.buffalo.cse562.queryplan;

import java.util.List;

public class ProjectNode implements Node {

	private List <String> columnList;
	private List <Node> nodeList;
	private Node childNode;
	
	public ProjectNode() {
				
	}
	
	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

	public List<Node> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<Node> nodeList) {
		this.nodeList = nodeList;
	}

	public Node getChildNode() {
		return childNode;
	}
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}	
	
	@Override
	public RelationNode eval() {		
		//TODO
		return null;
	}
}
