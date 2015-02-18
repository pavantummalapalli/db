package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class CartesianProduct {
	Node relationNode1;
	Node relationNode2;
	Expression expression;
	public CartesianProduct(Node relationNode1, Node relationNode2,
			EqualsTo expression) {
		this.relationNode1 = relationNode1;
		this.relationNode2 = relationNode2;
		this.expression = expression;
	}
	
	public RelationNode doCartesianProduct() {
		CreateTable table1 = relationNode1.eval().getSchema();
		CreateTable table2 = relationNode2.eval().getSchema();
		SqlIterator sqlIterator1 = new SqlIterator(table1, null);
		String newTableName = relationNode1.eval().getTableName() + "x" + relationNode2.eval().getTableName();
		String[] colVals1, colVals2;
		//TODO point the file to a temp location
		File file = new File(TableUtils.getDataDir() + File.separator + newTableName + ".dat");
		try {
			PrintWriter pw = new PrintWriter(file);
			while((colVals1 = sqlIterator1.next()) != null) {
				SqlIterator sqlIterator2 = new SqlIterator(table2, null);
				while((colVals2 = sqlIterator2.next()) != null) {
					int i;
					for(i=0; i<colVals1.length; i++) {
						pw.print(colVals1[i] + "|");
					}
					for(i=1; i<colVals2.length; i++) {
						pw.print(colVals2[i-1] + "|");
					}
					if(colVals2.length > 0)
						pw.println(colVals2[i-1]);
				}
				sqlIterator2.close();
			}
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		sqlIterator1.close();
		List<ColumnDefinition> list1 = table1.getColumnDefinitions();
		list1.addAll(table2.getColumnDefinitions());
		CreateTable newTable = new CreateTable();
		newTable.setTable(new Table(null, newTableName));
		newTable.setColumnDefinitions(list1);
		//TODO put the table name in a temp hash map
		TableUtils.getTableSchemaMap().put(newTableName, newTable);
		RelationNode relationNode = new RelationNode(newTableName, null,file,newTable);
		return relationNode;
	}
}
