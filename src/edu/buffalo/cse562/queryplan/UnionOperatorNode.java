package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class UnionOperatorNode implements Node {

	private Node parentNode;
	private List<Node> childNodes;
	
	public void setChildNodes(List<Node> childNodes) {
		this.childNodes = childNodes;
		Iterator<Node> nodesIt = childNodes.iterator();
		while(nodesIt.hasNext()){
			nodesIt.next().setParentNode(this);
		}
	}

	public List<Node> getChildNodes() {
		return childNodes;
	}

	@Override
	public RelationNode eval() {

		PrintWriter out = new PrintWriter(System.out);
		try {
			for (Node childNode : childNodes) {
				RelationNode relationNode = childNode.eval();
				//FileReader fileReader = new FileReader(relationNode.getFile().getReader());
				BufferedReader bufferedReader = new BufferedReader(relationNode.getFile().getReader());
				String rowVal;
								
				while ((rowVal = bufferedReader.readLine()) != null) {
					if (rowVal.trim().isEmpty())
						continue;
					out.println(rowVal);
				}
				bufferedReader.close();
				//fileReader.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		out.close();
		//TODO as of now returning nothing. Just printing.
		return null;
	}

	@Override
	public CreateTable evalSchema() {
		return null;
	}

	@Override
	public Node getParentNode() {
		return parentNode;
	}

	@Override
	public void setParentNode(Node parentNode) {
		this.parentNode=parentNode;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer("Union");
		Iterator<Node> nodesIt = childNodes.iterator();
		while(nodesIt.hasNext()){
			buffer.append(nodesIt.next().toString()+"\n");
		}
		return buffer.toString();
	}
}
