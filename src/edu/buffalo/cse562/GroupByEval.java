package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class GroupByEval {
	Node node;
	HashMap<String, Double> groupByCols;
	List<Column> items;
	List<String> aggregations;
	public GroupByEval(Node node, List<Column> items, List<String> aggregations) {
		this.node = node;
		this.items = items;
		this.aggregations = aggregations;
		groupByCols = new HashMap<>();
	}
	
	public RelationNode doCartesianProduct() {
		RelationNode relationNode = node.eval();
		CreateTable table = relationNode.getTable();
		File dataFile = relationNode.getFile();
		SqlIterator sqlIterator = new SqlIterator(table, null, dataFile);
		String newTableName = node.eval().getTableName() + "_groupby";
		String[] colVals;
		File file = new File(TableUtils.getDataDir() + File.separator + newTableName + ".dat");
		for(Column item:items) {
			
		}
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
		List<ColumnDefinition> list1 = table.getColumnDefinitions();
		CreateTable newTable = new CreateTable();
		newTable.setTable(new Table(null, newTableName));
		newTable.setColumnDefinitions(list1);
		TableUtils.getTableSchemaMap().put(newTableName, newTable);
		//RelationNode relationNode = new RelationNode(newTableName, null);
		//return relationNode;
		return null;
	}

}
