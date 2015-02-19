package edu.buffalo.cse562.queryplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse562.SqlIterator;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ExtendedProjectNode implements Node {

	private List <Function> functionList;
	private List <String> groupByList;
	private Node childNode;
	private Expression expression;
	private String delimiter = "~~";//delimiter to be decided
	
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}
	public Node getChildNode() {
		return childNode;
	}
	
	@Override
	public RelationNode eval() {
		Map<String, Double> groupByMap = new LinkedHashMap<>();
		RelationNode relationNode = childNode.eval();
		SqlIterator sqlIterator = new SqlIterator(relationNode.getTable(), expression, relationNode.getFile());
		String values[];
		List <ColumnDefinition> columnDefList = relationNode.getTable().getColumnDefinitions();
		Map <String, Integer> columnIndexMap = new HashMap <>();
		int cnt = 0;
		for (ColumnDefinition columnDef : columnDefList) {
			columnIndexMap.put(columnDef.getColumnName(), cnt++);
		}
		StringBuilder groupByCols = new StringBuilder("");
		while((values = sqlIterator.next()) != null) {
			for(String group : groupByList) {
				int index = columnIndexMap.get(group);
				groupByCols.append(values[index] + delimiter); 
			}
			String groupByColsStr = groupByCols.substring(0, groupByCols.length()-delimiter.length());
			if(groupByMap.containsKey(groupByColsStr)) {
				Double val = groupByMap.get(groupByColsStr);
				// TODO: values to be updated based on function name
				groupByMap.put(groupByColsStr, val);
			}
			else {
				// TODO: values to be updated based on function name
				groupByMap.put(groupByColsStr, 1.0);
			}
		}
		return relationNode;
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
	
	public Expression getExpression() {
		return expression;
	}
	
	public void setExpression(Expression expression) {
		this.expression = expression;
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
