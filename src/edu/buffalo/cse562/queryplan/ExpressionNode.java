package edu.buffalo.cse562.queryplan;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.SqlIterator;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ExpressionNode implements Node {

	private Expression expression;
	private Node evaluatedNode;
	private Node childNode;
	
	public ExpressionNode(Expression expression) {
		this.expression=expression;
	}
	
	@Override
	public RelationNode eval() {
		RelationNode relationNode = childNode.eval();
		String tableName = relationNode.getTableName();
		CreateTable table = TableUtils.getTableSchemaMap().get(tableName);
		SqlIterator sqlIterator = new SqlIterator(table, expression);
		String newTableName = tableName + "_new";
		String[] colVals;
		File file = new File(TableUtils.getDataDir() + File.separator + newTableName + ".dat");
		try {
			PrintWriter pw = new PrintWriter(file);
			while((colVals = sqlIterator.next()) != null) {
				int i;
				for(i=1; i<colVals.length; i++) {
					pw.print(colVals[i-1] + "|");
				}
				if(colVals.length > 0)
					pw.println(colVals[i-1]);
			}
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		sqlIterator.close();
		//file.renameTo(new File(TableUtils.getDataDir() + File.separator + tableName + ".dat"));
		relationNode.setTableName(newTableName);
		return relationNode;
	}
	
	public void setChildNode(Node childNode) {
		this.childNode = childNode;
	}
	public Node getChildNode() {
		return childNode;
	}
}
