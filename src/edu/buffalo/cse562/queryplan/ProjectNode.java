package edu.buffalo.cse562.queryplan;

import java.util.List;

import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectNode implements Node {

	private List <SelectItem> selectItems;
	private Node node;
	private Node childNode;

	public ProjectNode(List <SelectItem> selectItems) {
		this.selectItems = selectItems;
	}
	
	public Node getChildNode() {
		return childNode;
	}
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}
	
	@Override
	public RelationNode eval() {
		
		return null;
	}

}
