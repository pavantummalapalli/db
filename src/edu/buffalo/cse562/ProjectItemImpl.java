package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.queryplan.ExpressionNode;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.utils.TableUtils;

public class ProjectItemImpl implements SelectItemVisitor {
	
	//TODO move these static variables in props file.
	private static String DOT_STR = ".";
	private static String TABLE_SPLITTER_STR = "-";
	
	private Node node;
	private String[] tableName;
	private List <String> columnList;
	
	public ProjectItemImpl(String tableName) {
		// TODO Auto-generated constructor stub
		this.tableName = tableName.split(TABLE_SPLITTER_STR);
		this.columnList = new ArrayList <>();
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
		getColumnList();		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		String tname = allTableColumns.getTable().getName();
		this.tableName = new String[] {tname};		
		getColumnList();
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		//TODO
		ExpressionNode expNode = new ExpressionNode(selectExpressionItem.getExpression());
		this.node = expNode;
		
		//this.columnList.add(expNode.eval().getTableName() + DOT_STR + colName);
	}
	
	private void getColumnList() {
		for (String table : tableName) {
			List <ColumnDefinition> colDefList = TableUtils.getTableSchemaMap().get(table).getColumnDefinitions();
			for (ColumnDefinition colDef : colDefList) {
				this.columnList.add(tableName + DOT_STR + colDef.getColumnName());
			}
		}	
	}
	
	public Node getSelectItemNode() {
		return node;
	}
	
	public List <String> getSelectColumnList() {
		return columnList;
	}
}
