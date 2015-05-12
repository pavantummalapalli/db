package edu.buffalo.cse562.queryplan;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.DataSourceSqlIterator;
import edu.buffalo.cse562.GroupBy;
import edu.buffalo.cse562.datasource.BufferDataSource;
import edu.buffalo.cse562.datasource.DataSource;
import edu.buffalo.cse562.datasource.DataSourceWriter;
import edu.buffalo.cse562.datasource.FileDataSource;
import edu.buffalo.cse562.utils.TableUtils;

public class ExtendedProjectNode implements Node {

	private Node parentNode;
	private List<SelectExpressionItem> functionList;
	private List<Column> groupByList;
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
		childNode.setParentNode(this);
	}

	public Node getChildNode() {
		return childNode;
	}

	@Override
	public RelationNode eval() {
		try {
		RelationNode relationNode = childNode.eval();
		List<ColumnDefinition> columnDefList = relationNode.getTable()
				.getColumnDefinitions();
		Map<String, Integer> columnIndexMap = new HashMap<>();
		Map<String, ColumnDefinition> columnDefnMap = new HashMap<>();
		List<String> functionTypeList = new ArrayList<>();
		int cnt = 0;
		for (ColumnDefinition columnDef : columnDefList) {
			columnIndexMap.put(columnDef.getColumnName(), cnt++);
			columnDefnMap.put(columnDef.getColumnName(), columnDef);
		}

		List<Expression> expressionList = TableUtils
				.convertSelectExpressionItemIntoExpressions(functionList);
		DataSourceSqlIterator sqlIter = new DataSourceSqlIterator(
				relationNode.getTable(), expressionList,
				relationNode.getFile().getReader(), groupByList,
				relationNode.getExpression());
		sqlIter.nextAggregate();
		
			relationNode.getFile().clear();
			if (functionList.size() > 0) {
				String newTableName = relationNode.getTableName() + "_groupby";
				DataSource file = null;
				RelationNode relationNodeNew = new RelationNode();

				// Create Column Definitions
				List<ColumnDefinition> newList = new ArrayList<>();
				if (groupByList != null) {
					for (Column group : groupByList) {
						ColumnDefinition cd = columnDefnMap.get(group.getWholeColumnName());
						newList.add(cd);
					}
				}
				Map<GroupBy, Object> aggDataMap = sqlIter.getAggregateData(0);
				GroupBy tempKey = aggDataMap.keySet().iterator().next();
				for (int i = 0; i < functionList.size(); i++) {
					aggDataMap = sqlIter.getAggregateData(i);
					Object val = aggDataMap.get(tempKey);
					ColumnDefinition cd = new ColumnDefinition();
					SelectExpressionItem funcName = functionList.get(i);
					if (funcName.getAlias() != null
							&& !funcName.getAlias().isEmpty())
						cd.setColumnName(funcName.getAlias());
					else
						cd.setColumnName(funcName.toString());
					String type = null;
					if (val instanceof String)
						type = "string";
					else if (val instanceof Long || val instanceof Integer)
						type = "int";
					else if (val instanceof Double)
						type = "double";
					else if (val instanceof DateValue || val instanceof Date)
						type = "date";
					ColDataType cdt = new ColDataType();
					cdt.setDataType(type);
					cd.setColDataType(cdt);
					newList.add(cd);
				}
				
				relationNodeNew.setTableName(newTableName);
				CreateTable newTable = new CreateTable();
				newTable.setTable(new Table(null, newTableName));
				newTable.setColumnDefinitions(newList);
				relationNodeNew.setTable(newTable);
				if (TableUtils.isSwapOn)
					file = new FileDataSource(new File(
							TableUtils.getTempDataDir() + File.separator
 + newTableName + ".dat"), newList, newTableName);
				else
					file = new BufferDataSource();
				// End of setting the metaData
				relationNodeNew.setFile(file);
				DataSourceWriter fileWriter = file.getWriter();
				Map<String, Integer> columnMapping = TableUtils
						.getColumnMapping(newList);
				for (GroupBy key : aggDataMap.keySet()) {
					int z = 0;// tuple index
					int groupBySize=groupByList!=null?groupByList.size():0;
					int functionListSize = functionList!=null?functionList.size():0;
					LeafValue[] tuple = new LeafValue[groupBySize+functionListSize];
					if (key != null) {
						LeafValue colsVals[] = key.getLeafValue();
						for (int i = 0; i < colsVals.length; i++, z++) {
							tuple[z] = colsVals[i];
						}
					}
					for (int i = 0; i < functionList.size(); i++, z++) {
						aggDataMap = sqlIter.getAggregateData(i);
						Object val = aggDataMap.get(key);
						if (val == null) {
							throw new RuntimeException(
									" VAL IS NULL. aggDataMap  " + aggDataMap
											+ "   ");
						}
						tuple[z] = TableUtils.getIdentifiedLeafValue(val);
					}
					fileWriter.writeNextTuple(tuple);
				}
				fileWriter.close();
				return relationNodeNew;
			}
		return relationNode;
		} catch (IOException e) {
			// e.printStackTrace();
			throw new RuntimeException("FileNotFound exception ", e);
		}
	}

	public void setGroupByList(List<Column> groupByList) {
		this.groupByList = groupByList;
	}

	public List<Column> getGroupByList() {
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
		// List columnDef = new ArrayList();
		// if(groupByList!=null && groupByList.size()>0)
		// columnDef =
		// TableUtils.convertColumnNameToColumnDefinitions(groupByList);
		// if(functionList!=null && functionList.size()>0)
		// columnDef.addAll(TableUtils.convertFunctionNameToColumnDefinitions(functionList));
		// table.setColumnDefinitions(columnDef);
		return table;
	}

	@Override
	public Node getParentNode() {
		// TODO Auto-generated method stub
		return parentNode;
	}

	@Override
	public void setParentNode(Node parentNode) {
		// TODO Auto-generated method stub
		this.parentNode = parentNode;
	}

	@Override
	public String toString() {
		// StringBuffer buffer = new
		// StringBuffer("Aggregation : Functions - "+functionList!=null?functionList.toString():" Nothing "+" Group By : "+groupByList!=null?groupByList.toString():" Nothing "+
		// " Having : "+havingExpression!=null?havingExpression.toString():" Nothing \n"
		// );
		StringBuffer buffer = new StringBuffer("Aggregation \n");
		buffer.append(childNode.toString() + "\n");
		return buffer.toString();
	}
}
