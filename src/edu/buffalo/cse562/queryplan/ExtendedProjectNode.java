package edu.buffalo.cse562.queryplan;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ExtendedProjectNode implements Node {

	private List <Function> functionList;
	private List <String> groupByList;
	private Node childNode;
	
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}
	public Node getChildNode() {
		return childNode;
	}
	
	@Override
	public RelationNode eval() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void setGroupByList(List<String> groupByList) {
		this.groupByList = groupByList;
	}
	
	public List<String> getGroupByList() {
		return groupByList;
	}
	
	public List<Function> getFunctionList() {
		return functionList;
	}

	public void setFunctionList(List<Function> functionList) {
		this.functionList = functionList;
	}
	
	@Override
	public CreateTable evalSchema() {
		CreateTable table = new CreateTable();
		List columnDef = new ArrayList();
		if(groupByList!=null && groupByList.size()>0)
			columnDef = TableUtils.convertColumnNameToColumnDefinitions(groupByList);
		if(functionList!=null && functionList.size()>0)
			columnDef.addAll(TableUtils.convertFunctionNameToColumnDefinitions(functionList));
		table.setColumnDefinitions(columnDef);
		return table;
	}
}
