package edu.buffalo.cse562.queryplan;

import java.util.ArrayList;
import java.util.HashMap;
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
	private Expression havingExpression;
	private Node childNode;
	
	public void setHavingExpression(Expression havingExpression) {
		this.havingExpression = havingExpression;
	}
	public Expression getHavingExpression() {
		return havingExpression;
	}
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}
	public Node getChildNode() {
		return childNode;
	}
	
	@Override
	public RelationNode eval() {
		RelationNode relationNode = childNode.eval();
		List<ColumnDefinition> columnDefList = relationNode.getTable().getColumnDefinitions();
		Map<String, Integer> columnIndexMap = new HashMap<>();
		int cnt = 0;
		for (ColumnDefinition columnDef : columnDefList) {
			columnIndexMap.put(columnDef.getColumnName(), cnt++);
		}
		List<Expression> expressionList = (List<Expression>) (List<?>) functionList;
		SqlIterator sqlIter = new SqlIterator(relationNode.getTable(), expressionList, relationNode.getFile(),
				groupByList);

		while (sqlIter.nextAggregate() != null) {
			// do nothing.
		}
		for (int i = 0; i < functionList.size(); i++) {
			Map<String, Object> aggDataMap = sqlIter.getAggregateData(i);
			for (String key : aggDataMap.keySet()) {
				System.out.println(aggDataMap.get(key));
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
