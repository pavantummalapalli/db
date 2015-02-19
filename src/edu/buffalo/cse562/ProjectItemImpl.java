package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.queryplan.Node;

public class ProjectItemImpl implements SelectItemVisitor {
	
	//TODO move these static variables in props file.
	private static String DOT_STR = ".";
		
	private Node node;
	
	//not using right now.
	//private List <String> tableList;
	
	private Map <String, String> columnTableMap;
	
	private List <String> columnList;

	private List <Function> functionList;
	
	public ProjectItemImpl(List <String> tableList, Map <String, String> columnTableMap) {
		// TODO Auto-generated constructor stub
		//this.tableList = tableList;
		this.columnTableMap = columnTableMap;
		
		//TODO on demand init
		this.columnList = new ArrayList<>();
		this.functionList = new ArrayList <>();
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
		columnList = null;		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported as of now.");
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		//TODO
		Expression expression = selectExpressionItem.getExpression();
		if (expression instanceof Column) {
			Column column = (Column)expression;
			String columnStr = column.getWholeColumnName();
			String resolvedColumn = columnStr;
			
			if (column.getTable() == null || column.getTable().getName() == null || column.getTable().getName().isEmpty()) {
				resolvedColumn = columnTableMap.get(columnStr) + DOT_STR + columnStr;
			}	
			columnList.add(resolvedColumn);
		} else if (expression instanceof Function) {
			functionList.add((Function)expression);
		}
	}
	
	/*private void getColumnList() {
		for (String table : tableList) {
			List <ColumnDefinition> colDefList = TableUtils.getTableSchemaMap().get(table).getColumnDefinitions();
			for (ColumnDefinition colDef : colDefList) {
				String columnName = colDef.getColumnName();
				if (!columnName.contains(DOT_STR)) {
					table = table + DOT_STR + colDef.getColumnName();
				}	
				columnList.add(table + DOT_STR + colDef.getColumnName());
			}
		}
	}*/
	
	public Node getSelectItemNode() {
		return node;
	}
	
	public List <String> getSelectColumnList() {
		return columnList;
	}
	public List <Function> getFunctionList() {
		return functionList;
	}
}
