package edu.buffalo.cse562.queryplan;

import static edu.buffalo.cse562.utils.TableUtils.toUnescapedString;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.DataSourceSqlIterator;
import edu.buffalo.cse562.utils.TableUtils;
public class ProjectNode implements Node {

	private Node parentNodeLink;
	private boolean parentNode = true;
	private List<SelectExpressionItem> selectItemsList;
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
		this.selectItemsList = expressionList;
	}

	public List<SelectExpressionItem> getExpressionList() {
		return selectItemsList;
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
		childNode.setParentNode(this);
	}

	@Override
	public RelationNode eval() {
		RelationNode relationNode = childNode.eval();
		try{
		DataSourceSqlIterator iterator = new DataSourceSqlIterator(
				relationNode.getTable(),
				TableUtils
						.convertSelectExpressionItemIntoExpressions(selectItemsList),
				relationNode.getFile(), null,relationNode.getExpression());
		List<ColumnDefinition> columnDefList = TableUtils.convertSelectExpressionItemsIntoColumnDefinition(selectItemsList);
		//List<ColumnDefinition> columnDefList = relationNode.getTable()
		//		.getColumnDefinitions();
		Map<String, Integer> columnIndexMap = new HashMap<>();
		//Map<String, ColumnDefinition> columnDefnMap = new HashMap<>();
		int cnt = 0;
		for (ColumnDefinition columnDef : columnDefList) {
			columnIndexMap.put(columnDef.getColumnName().toUpperCase(), cnt++);
			//columnDefnMap.put(columnDef.getColumnName().toUpperCase(),
			//		columnDef);
		}
		LeafValue[] colVals;
		List<LeafValue[]> projectList = new ArrayList<>();
		iterator.open();
		List<LeafValue> projectedSchema = new ArrayList<LeafValue>();
		boolean iteratedOnce=false;
		while ((colVals = iterator.next()) != null) {
			if(!iteratedOnce){
				for(int i=0;i<colVals.length;i++){
					projectedSchema.add(colVals[i]);
				}
				iteratedOnce=true;
			}
			projectList.add(colVals);
		}
		relationNode.getFile().close();
		System.gc();
		DataSource file = null;
		PrintWriter pw = null;
		
		if (parentNode == false) {
			String newTableName = relationNode.getTableName() + "_new";
			if(TableUtils.isSwapOn){
				file = new FileDataSource(new File(TableUtils.getTempDataDir() + File.separator
						+ newTableName + ".dat"));
			}
			else{
				file = new BufferDataSource();
			}
			pw = new PrintWriter(file.getWriter());
		}
		if (orderByElements != null && orderByElements.size() > 0) {
			Collections.sort(projectList, new Comparator<LeafValue[]>() {
				@Override
				public int compare(LeafValue[] o1, LeafValue[] o2) {
					
					for (OrderByElement element : orderByElements) {
						boolean isAsc = element.isAsc();
						String orderByColumnName = element.toString();
						if (!isAsc)
							orderByColumnName = orderByColumnName.split(" ")[0]
									.trim();
						int index = columnIndexMap.get(orderByColumnName);
						LeafValue val1 = o1[index];
						LeafValue val2 = o2[index];
						int compareValue= TableUtils.compareTwoLeafValues(val1, val2);
						if (compareValue==0)
							continue;
						return isAsc ?compareValue : TableUtils.compareTwoLeafValues(val2, val1);
					}
					return 0;
				}
			});
		}
		// Expand column during runtime
		if (selectItemsList.size() == 1 && selectItemsList.get(0).equals("*")) {
			selectItemsList.remove(0);
			selectItemsList
					.addAll(TableUtils.convertColumnListIntoSelectExpressionItem(columnIndexMap
							.keySet()));
		}

		// TODO distinct and Column resolution is not +nt.
		long offset = (limit == null ? Integer.MAX_VALUE : limit.getRowCount());
		List<String> columnList = TableUtils
				.convertSelectExpressionItemIntoColumnString(selectItemsList);
		
		Iterator<LeafValue[]> it = projectList.iterator();
		
		while (it.hasNext() && offset-- > 0) {
			LeafValue[] rowArr = it.next();
			int j = 0;
			for (; j < columnList.size() - 1; j++) {
				if (parentNode)
					System.out.print(toUnescapedString(rowArr[j]) + "|");
				else
					pw.print(toUnescapedString(rowArr[j]) + "|");
			}
			if (columnList.size() > 0) {
				if (parentNode)
					System.out.println(toUnescapedString(rowArr[j]));
				else
					pw.println(toUnescapedString(rowArr[j]));
			}
			it.remove();
		}
		projectList.clear();
		System.gc();
		if (parentNode == false) {
			pw.close();
			relationNode.setFile(file);
			List<ColumnDefinition> newList = new ArrayList<>();
			int i=0;
			List<Column> projectedColumns =TableUtils.convertSelectExpressionItemIntoColumn(selectItemsList);
			for (LeafValue column : projectedSchema) {
				ColumnDefinition cd = new ColumnDefinition();
				cd.setColumnName(projectedColumns.get(i).getColumnName());
				ColDataType type = new ColDataType();
				if(column instanceof LongValue)
					type.setDataType("int");
				else if(column instanceof DoubleValue)
					type.setDataType("double");
				else if(column instanceof StringValue)
					type.setDataType("string");	
				else if(column instanceof DateValue)
					type.setDataType("date");	
				cd.setColDataType(type);
				newList.add(cd);
				i++;
			}
			CreateTable newTable = new CreateTable();
			newTable.setTable(new Table(null, preferredAliasName));
			newTable.setColumnDefinitions(newList);
			RelationNode relationNode1 = new RelationNode(preferredAliasName,
					preferredAliasName, file, newTable);
			return relationNode1;
		}
		}catch(IOException e){
			throw new RuntimeException("File not found",e);
		}
		return null;
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

	@Override
	public Node getParentNode() {
		return parentNodeLink;
	}

	@Override
	public void setParentNode(Node parentNode) {
		this.parentNodeLink=parentNode;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer("Project Data");
		buffer.append("\n");
		buffer.append(childNode.toString());
		buffer.append("\n");
		return buffer.toString();
	}
}
