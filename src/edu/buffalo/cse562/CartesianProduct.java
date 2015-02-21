package edu.buffalo.cse562;

import static edu.buffalo.cse562.utils.TableUtils.convertColumnDefinitionIntoSelectExpressionItems;
import static edu.buffalo.cse562.utils.TableUtils.convertSelectExpressionItemIntoExpressions;
import static edu.buffalo.cse562.utils.TableUtils.toUnescapedString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class CartesianProduct {
	Node node1;
	Node node2;
	Expression expression;
	public CartesianProduct(Node node1, Node node2,
			EqualsTo expression) {
		this.node1 = node1;
		this.node2 = node2;
		this.expression = expression;
	}
	
	public RelationNode doCartesianProduct() {
		RelationNode relationNode1 = node1.eval();
		RelationNode relationNode2 = node2.eval();
		CreateTable table1 = relationNode1.getTable();
		CreateTable table2 = relationNode2.getTable();
		List<SelectExpressionItem> items = convertColumnDefinitionIntoSelectExpressionItems(table1.getColumnDefinitions());
		File dataFile1 = relationNode1.getFile();
		File dataFile2 = relationNode2.getFile();
		List<Expression> table1ItemsExpression = convertSelectExpressionItemIntoExpressions(items);
		SqlIterator sqlIterator1 = new SqlIterator(table1,table1ItemsExpression , dataFile1,null);
		String newTableName = getNewTableName(table1, table2);
		LeafValue[] colVals1, colVals2;
		File file = new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat");
		try {
			PrintWriter pw = new PrintWriter(file);
			while((colVals1 = sqlIterator1.next()) != null) {
				SqlIterator sqlIterator2 = new SqlIterator(table2,convertSelectExpressionItemIntoExpressions( TableUtils.convertColumnDefinitionIntoSelectExpressionItems(table2.getColumnDefinitions())), dataFile2,null);
				while((colVals2 = sqlIterator2.next()) != null) {
					int i;
					for(i=0; i<colVals1.length; i++) {
						pw.print(toUnescapedString(colVals1[i]) + "|");
					}
					for(i=1; i<colVals2.length; i++) {
						pw.print(toUnescapedString(colVals2[i-1]) + "|");
					}
					if(colVals2.length > 0)
						pw.println(toUnescapedString(colVals2[i-1]));
				}
				sqlIterator2.close();
			}
			pw.close();
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		sqlIterator1.close();
		List<ColumnDefinition> newList = new ArrayList<ColumnDefinition>();
		newList.addAll(table1.getColumnDefinitions());
		newList.addAll(table2.getColumnDefinitions());
		CreateTable newTable = new CreateTable();
		newTable.setTable(new Table(null, newTableName));
		newTable.setColumnDefinitions(newList);
		//TODO put the table name in a temp hash map
		//TableUtils.getTableSchemaMap().put(newTableName, newTable);
		RelationNode relationNode = new RelationNode(newTableName, null,file,newTable);
		return relationNode;
	}
	
	private String getNewTableName(CreateTable table1,CreateTable table2){
		return table1.getTable().getName() + "x" + table2.getTable().getName();
	}
	
	public CreateTable evalSchema(){
		CreateTable table1 = node1.evalSchema();
		CreateTable table2 = node2.evalSchema();
		CreateTable table = new CreateTable();
		table.setTable(new Table());
		List<ColumnDefinition> list = new LinkedList<>();
		list.addAll(table1.getColumnDefinitions());
		list.addAll(table2.getColumnDefinitions());
		table.setColumnDefinitions(list);
		return table;
	}
}
