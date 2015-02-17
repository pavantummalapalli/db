package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
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
		CreateTable table1 = TableUtils.getTableSchemaMap().get(relationNode1.eval().getTableName());
		CreateTable table2 = TableUtils.getTableSchemaMap().get(relationNode2.eval().getTableName());
		SqlIterator sqlIterator1 = new SqlIterator(table1, null);
		String newTableName = relationNode1.eval().getTableName() + "x" + relationNode2.eval().getTableName();
		List<String> colVals1, colVals2;
		File file = new File(TableUtils.getDataDir() + File.separator + newTableName + ".dat");
		try {
			PrintWriter pw = new PrintWriter(file);
			while((colVals1 = sqlIterator1.next()) != null) {
				SqlIterator sqlIterator2 = new SqlIterator(table2, null);
				while((colVals2 = sqlIterator2.next()) != null) {
					colVals1.addAll(colVals2);
					Iterator<String> it = colVals1.iterator();
					while(it.hasNext()) {
						String value = it.next();
						if(it.hasNext())
							pw.print(value + "|");
						else
							pw.println(value);
					}
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
		TableUtils.getTableSchemaMap().put(newTableName, newTable);
		RelationNode relationNode = new RelationNode(newTableName, null);
		return relationNode;
	}

}
