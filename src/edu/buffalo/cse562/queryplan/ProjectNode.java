package edu.buffalo.cse562.queryplan;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.SqlIterator;
import edu.buffalo.cse562.utils.TableUtils;

public class ProjectNode implements Node {

	private boolean parentNode = true;
	private List<SelectExpressionItem> expressionList;
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

	public void setExpressionList(List<SelectExpressionItem> expressionList) {
		this.expressionList = expressionList;
	}

	public List<SelectExpressionItem> getExpressionList() {
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

	public Node getChildNode() {
		return childNode;
	}

	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}

	@Override
	public RelationNode eval() {
		RelationNode relationNode = childNode.eval();
		try{
		SqlIterator iterator = new SqlIterator(
				relationNode.getTable(),
				TableUtils
						.convertSelectExpressionItemIntoExpressions(expressionList),
				relationNode.getFile(), null);

		List<ColumnDefinition> columnDefList = relationNode.getTable()
				.getColumnDefinitions();
		Map<String, Integer> columnIndexMap = new HashMap<>();
		Map<String, ColumnDefinition> columnDefnMap = new HashMap<>();
		List<String> functionTypeList = new ArrayList<>();
		int cnt = 0;
		for (ColumnDefinition columnDef : columnDefList) {
			columnIndexMap.put(columnDef.getColumnName().toUpperCase(), cnt++);
			columnDefnMap.put(columnDef.getColumnName().toUpperCase(),
					columnDef);
		}
		String[] colVals;
		List<String[]> projectList = new ArrayList<>();
		iterator.open();
		while ((colVals = iterator.next()) != null) {
			projectList.add(colVals);
		}
		File file = null;
		PrintWriter pw = null;

		if (parentNode == false) {
			String newTableName = relationNode.getTableName() + "_new";
			file = new File(TableUtils.getTempDataDir() + File.separator
					+ newTableName + ".dat");
			pw = new PrintWriter(file);
		}

		if (orderByElements != null && orderByElements.size() > 0) {
			Collections.sort(projectList, new Comparator<String[]>() {
				@Override
				public int compare(String[] o1, String[] o2) {
					for (OrderByElement element : orderByElements) {
						boolean isAsc = element.isAsc();
						String orderByColumnName = element.toString();
						if (!isAsc)
							orderByColumnName = orderByColumnName.split(" ")[0]
									.trim();
						int index = columnIndexMap.get(orderByColumnName);
						String val1 = o1[index];
						String val2 = o2[index];
						if (val1.equals(val2))
							continue;
						return isAsc ? val1.compareTo(val2) : val2
								.compareTo(val1);
					}
					return 0;
				}
			});
		}
		// Expand column during runtime
		if (expressionList.size() == 1 && expressionList.get(0).equals("*")) {
			expressionList.remove(0);
			expressionList
					.addAll(TableUtils.convertColumnIntoSelectExpressionItem(columnIndexMap
							.keySet()));
		}

		// TODO distinct and Column resolution is not +nt.
		long offset = (limit == null ? Integer.MAX_VALUE : limit.getRowCount());
		List<String> columnList = TableUtils
				.convertSelectExpressionItemIntoColumnString(expressionList);
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
				int index = columnIndexMap
						.get(columnList.get(columnList.size() - 1));
				if (parentNode)
					System.out.println(rowArr[index]);
				else
					pw.println(rowArr[index]);
			}
		}
		if (parentNode == false) {
			pw.close();
			relationNode.setFile(file);
			List<ColumnDefinition> newList = new ArrayList<>();
			for (String column : columnList) {
				ColumnDefinition cd = columnDefnMap.get(column);
				newList.add(cd);
			}
			if (preferredAliasName != null
					&& preferredAliasName.isEmpty() == false)
				relationNode.setAliasName(preferredAliasName);
			if (relationNode.getTableName() == null
					|| relationNode.getTableName().isEmpty())
				relationNode.setTableName(preferredAliasName);
			CreateTable newTable = new CreateTable();
			newTable.setTable(new Table(null, preferredAliasName));
			newTable.setColumnDefinitions(newList);
			RelationNode relationNode1 = new RelationNode(preferredAliasName,
					preferredAliasName, file, newTable);
			relationNode1.setTable(newTable);
		}
		}catch(IOException e){
			throw new RuntimeException("File not found",e);
		}
		return relationNode;
	}

	@Override
	public CreateTable evalSchema() {
		CreateTable table = new CreateTable();
		List columnDef = new ArrayList();
		// if (columnList != null && columnList.size() > 0)
		// columnDef =
		// TableUtils.convertColumnNameToColumnDefinitions(columnList);
		// if (functionList != null && functionList.size() > 0)
		// columnDef.addAll(TableUtils.convertFunctionNameToColumnDefinitions(functionList));
		table.setColumnDefinitions(columnDef);
		return table;
	}
}
