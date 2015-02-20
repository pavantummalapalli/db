package edu.buffalo.cse562.queryplan;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.SqlIterator;
import edu.buffalo.cse562.utils.TableUtils;

public class ExtendedProjectNode implements Node {

	private List <SelectExpressionItem> functionList;
	private List <String> groupByList;
	private Expression havingExpression;
	private Node childNode;
	private String delimiter = "~~";//delimiter to be decided
	
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
		Map<String, ColumnDefinition> columnDefnMap = new HashMap<>();
		List<String> functionTypeList = new ArrayList<>();
		int cnt = 0;
		for (ColumnDefinition columnDef : columnDefList) {
			columnIndexMap.put(columnDef.getColumnName(), cnt++);
			columnDefnMap.put(columnDef.getColumnName(), columnDef);
		}

		List<Expression> expressionList = TableUtils.convertSelectExpressionItemIntoExpressions( functionList);
		SqlIterator sqlIter = new SqlIterator(relationNode.getTable(), expressionList, relationNode.getFile(),
				groupByList);

		while (sqlIter.nextAggregate() != null) {
			// do nothing.
		}
		int i = 0;
		if(functionList.size() > 0) {
			String newTableName = relationNode.getTableName() + "_groupby";
			String[] colVals1, colVals2;
			File file = new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat");
			try {
				PrintWriter pw = new PrintWriter(file);
				Map<String, Object> aggDataMap = sqlIter.getAggregateData(i);
				for (String key : aggDataMap.keySet()) {
					StringBuilder sb = new StringBuilder("");
					sb.append(key);
					for (i=0; i < functionList.size(); i++) {
						aggDataMap = sqlIter.getAggregateData(i);
						Object val = aggDataMap.get(key);
						sb.append("|");
						sb.append(val);
						if (val == null) {
							throw new RuntimeException(" VAL IS NULL. aggDataMap  " + aggDataMap + "   ");
						}
						if(val instanceof String) 
							functionTypeList.add("string");
						else if(val instanceof Long || val instanceof Integer)
							functionTypeList.add("int");
						else if(val instanceof Double)
							functionTypeList.add("double");
						else if(val instanceof DateValue || val instanceof Date)
							functionTypeList.add("date");
						}
					pw.println(sb.toString());
				}
				pw.close();
				List<ColumnDefinition> newList = new ArrayList<>();
				for(String group : groupByList) {
					ColumnDefinition cd = columnDefnMap.get(group);
					newList.add(cd);
				}
				int k=0;
				for(SelectExpressionItem funcName : functionList) {
					ColumnDefinition cd = new ColumnDefinition();
					Function func =(Function)funcName.getExpression(); 
					if(funcName.getAlias()!=null && !funcName.getAlias().isEmpty())
						cd.setColumnName(funcName.getAlias());
					else
						cd.setColumnName(funcName.toString());
					ColDataType cdt = new ColDataType();
					cdt.setDataType(functionTypeList.get(k));
					cd.setColDataType(cdt);
					newList.add(cd);
					k++;
				}
				relationNode.setFile(file);
				relationNode.setTableName(newTableName);
				CreateTable newTable = new CreateTable();
				newTable.setTable(new Table(null, newTableName));
				newTable.setColumnDefinitions(newList);
				relationNode.setTable(newTable);
			} catch (FileNotFoundException e) {
				//e.printStackTrace();
				throw new RuntimeException("FileNotFound exception ", e);
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
	
	public List<SelectExpressionItem> getFunctionList() {
		return functionList;
	}

	public void setFunctionList(List<SelectExpressionItem> functionList) {
		this.functionList = functionList;
	}
	
	@Override
	public CreateTable evalSchema() {
		CreateTable table = new CreateTable();
//		List columnDef = new ArrayList();
//		if(groupByList!=null && groupByList.size()>0)
//			columnDef = TableUtils.convertColumnNameToColumnDefinitions(groupByList);
//		if(functionList!=null && functionList.size()>0)
//			columnDef.addAll(TableUtils.convertFunctionNameToColumnDefinitions(functionList));
//		table.setColumnDefinitions(columnDef);
		return table;
	}
}
