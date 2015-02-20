package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.queryplan.QueryDomain;

public class ProjectItemImpl implements SelectItemVisitor {
	
	private List <String> columnList;
	private List <Function> functionList;
	private List <SelectExpressionItem> expressionList = new ArrayList<SelectExpressionItem>();
	private QueryDomain queryDomain;
	
	public ProjectItemImpl(QueryDomain queryDomain) {
		this.columnList = new ArrayList<>();
		this.functionList = new ArrayList <>();
		this.queryDomain=queryDomain;
	}
	
	public List<SelectExpressionItem> getExpressionList() {
		return expressionList;
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		columnList.add(allColumns.toString());
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		throw new UnsupportedOperationException("Not supported as of now.");
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		expressionList.add(selectExpressionItem);
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
	
//	public List <String> getSelectColumnList() {
//		return columnList;
//	}
//	
//	public List <Function> getFunctionList() {
//		return functionList;
//	}
}
