package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.SqlIterator;
import edu.buffalo.cse562.utils.TableUtils;

public class ProjectNode implements Node {

	private boolean parentNode = true;
	private List<String> columnList;
	private List<Function> functionList;
	private List<Expression> expressionList;
	private Limit limit;
	private Distinct distinctOnElements;
	private List<OrderByElement> orderByElements;
	private Node childNode;
	private String preferredAliasName;	
	
	public void setParentNode(boolean parentNode) {
		this.parentNode = parentNode;
	}
	public boolean isParentNode() {
		return parentNode;
	}
	
	public void setExpressionList(List<Expression> expressionList) {
		this.expressionList = expressionList;
	}

	public List<Expression> getExpressionList() {
		return expressionList;
	}

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

	// public List<Node> getNodeList() {
	// return nodeList;
	// }
	//
	// public void setNodeList(List<Node> nodeList) {
	// this.nodeList = nodeList;
	// }

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
			// FileReader fileReader = new FileReader(TableUtils.getDataDir() +
			// File.separator + tableName + ".dat");
			FileReader fileReader = new FileReader(relationNode.getFile());
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String rowVal;
			List<ColumnDefinition> columnDefList = relationNode.getTable().getColumnDefinitions();
			Map<String, Integer> columnIndexMap = new HashMap<>();
			Map<String, ColumnDefinition> columnDefnMap = new HashMap<>();
			List<String> functionTypeList = new ArrayList<>();

			int cnt = 0;
			for (ColumnDefinition columnDef : columnDefList) {
				columnIndexMap.put(columnDef.getColumnName(), cnt++);
				columnDefnMap.put(columnDef.getColumnName(), columnDef);
			}

			List<String[]> projectList = new ArrayList<>();
			while ((rowVal = bufferedReader.readLine()) != null) {
				if (rowVal.trim().isEmpty())
					continue;
				String[] colVals = rowVal.split("\\|");
				for (int i = 0; i < colVals.length; i++) {
					colVals[i] = colVals[i].trim();
				}
				projectList.add(colVals);
			}
			File file = null;
			PrintWriter pw = null;
			
			if (parentNode == false) {
				String newTableName = relationNode.getTableName() + "_new";
				file = new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat");
				pw = new PrintWriter(file);
			}	
			
			if (columnList != null && !columnList.isEmpty()) {
				if(orderByElements!=null && orderByElements.size()>0){
				Collections.sort(projectList, new Comparator<String[]>() {
					@Override
					public int compare(String[] o1, String[] o2) {
						for (OrderByElement element : orderByElements) {
							boolean isAsc = element.isAsc();
							String orderByColumnName = element.toString();
							if (!isAsc)
								orderByColumnName = orderByColumnName.split(" ")[0].trim();
							int index = columnIndexMap.get(orderByColumnName);
							String val1 = o1[index];
							String val2 = o2[index];
							if (val1.equals(val2))
								continue;
							return isAsc ? val1.compareTo(val2) : val2.compareTo(val1);
						}
						return 0;
					}
				});
				}
				//Expand column during runtime
				if(columnList.size()==1 && columnList.get(0).equals("*")){
					columnList.remove(0);
					columnList.addAll(columnIndexMap.keySet());
				}
				// TODO distinct and Column resolution is not +nt.
				long offset = (limit == null ? Integer.MAX_VALUE : limit.getRowCount());
				for (int i = 0; i < Math.min(offset, projectList.size()); i++) {
					String[] rowArr = projectList.get(i);
					for (int j = 0; j < columnList.size() - 1; j++) {
						String column = columnList.get(j);
						int index = columnIndexMap.get(column);
						if (parentNode)
							System.out.print(rowArr[index] + "|");
						else
							pw.print(rowArr[index] + "|");
							
					}
					if (columnList.size() > 0) {
						int index = columnIndexMap.get(columnList.get(columnList.size() - 1));
						if (parentNode)
							System.out.println(rowArr[index]);
						else
							pw.println(rowArr[index]);
					}
				}

			} else if (functionList != null && !functionList.isEmpty()) {

				// List <Expression> expressionList =
				// func.getParameters().getExpressions();
				// for (Expression expr : expressionList) {
				List<Expression> expressionList = (List<Expression>) (List<?>) functionList;
				SqlIterator sqlIter = new SqlIterator(relationNode.getTable(), expressionList, relationNode.getFile(),
						null);

				while (sqlIter.nextAggregate() != null) {
					// do nothing. processing happening inside nextAggregate for all the exprList per row.
				}
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < functionList.size(); i++) {
					Map<String, Object> aggDataMap = sqlIter.getAggregateData(i);
					
					for (String key : aggDataMap.keySet()) {
						Object val = aggDataMap.get(key);
						sb.append(val);
						sb.append("|");
						if(val instanceof String) 
							functionTypeList.add("string");
						else if(val instanceof Long || val instanceof Integer)
							functionTypeList.add("int");
						else if(val instanceof Double)
							functionTypeList.add("double");
						else if(val instanceof DateValue || val instanceof Date)
							functionTypeList.add("date");
					}						
				}
				if (sb.length() > 0) {
					if (parentNode)
						System.out.println(sb.substring(0, sb.length() - 1));
					else
						pw.println(sb.substring(0, sb.length() - 1));
				}
			}
			if (parentNode == false) {
				pw.close();
				relationNode.setFile(file);
				List<ColumnDefinition> newList = new ArrayList<>();
				for(String column : columnList) {
					ColumnDefinition cd = columnDefnMap.get(column);
					newList.add(cd);
				}
				int k=0;
				for(Function funcName : functionList) {
					ColumnDefinition cd = new ColumnDefinition();
					cd.setColumnName(funcName.toString());
					ColDataType cdt = new ColDataType();
					cdt.setDataType(functionTypeList.get(k));
					cd.setColDataType(cdt);
					newList.add(cd);
					k++;
				}
				if (preferredAliasName != null && preferredAliasName.isEmpty() == false)
					relationNode.setAliasName(preferredAliasName);
				if (relationNode.getTableName() == null || relationNode.getTableName().isEmpty())
					relationNode.setTableName(preferredAliasName);
				CreateTable newTable = new CreateTable();
				newTable.setTable(new Table(null, preferredAliasName));
				newTable.setColumnDefinitions(newList);
				relationNode.setTable(newTable);
			}
			bufferedReader.close();
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return relationNode;
	}

	@Override
	public CreateTable evalSchema() {
		CreateTable table = new CreateTable();
		List columnDef = new ArrayList();
		if (columnList != null && columnList.size() > 0)
			columnDef = TableUtils.convertColumnNameToColumnDefinitions(columnList);
		if (functionList != null && functionList.size() > 0)
			columnDef.addAll(TableUtils.convertFunctionNameToColumnDefinitions(functionList));
		table.setColumnDefinitions(columnDef);
		return table;
	}
}
