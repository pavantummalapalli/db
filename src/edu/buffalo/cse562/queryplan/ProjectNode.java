package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class ProjectNode implements Node {

	private List <String> columnList;
	private List <Function> functionList;
	private Limit limit;
	private Distinct distinctOnElements;
	private List<OrderByElement> orderByElements;
	private Node childNode;
	private String preferredAliasName;
	
	
	public void setOrderByElements(List<OrderByElement> orderByElements) {
		this.orderByElements = orderByElements;
	}
	
	public List<OrderByElement> getOrderByElements() {
		return orderByElements;
	}
	
	public void setLimit(Limit limit) {
		this.limit = limit;
	}
	public Limit getLimit() {
		return limit;
	}
	public void setDistinctOnElements(Distinct distinctOnElements) {
		this.distinctOnElements = distinctOnElements;
	}
	public Distinct getDistinctOnElements() {
		return distinctOnElements;
	}

	public void setPreferredAliasName(String preferredAliasName) {
		this.preferredAliasName = preferredAliasName;
	}
	
	public String getPreferredAliasName() {
		return preferredAliasName;
	}
	
	public ProjectNode() {
				
	}
	
	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

//	public List<Node> getNodeList() {
//		return nodeList;
//	}
//
//	public void setNodeList(List<Node> nodeList) {
//		this.nodeList = nodeList;
//	}

	public Node getChildNode() {
		return childNode;
	}
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}	
	public List<Function> getFunctionList() {
		return functionList;
	}

	public void setFunctionList(List<Function> functionList) {
		this.functionList = functionList;
	}
	
	@Override
	public RelationNode eval() {
		RelationNode relationNode = childNode.eval();
		try {
			//FileReader fileReader = new FileReader(TableUtils.getDataDir() + File.separator + tableName + ".dat");
			FileReader fileReader = new FileReader(relationNode.getFilePath());
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String rowVal;
			while((rowVal = bufferedReader.readLine()) != null) {
				System.out.println(rowVal);
			}
			bufferedReader.close();
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		relationNode.setAliasName(preferredAliasName);
		if(relationNode.getTableName()==null || relationNode.getTableName().isEmpty())
			relationNode.setTableName(preferredAliasName);
		return relationNode;
	}
}
