package edu.buffalo.cse562;

import static edu.buffalo.cse562.utils.TableUtils.convertColumnDefinitionIntoSelectExpressionItems;
import static edu.buffalo.cse562.utils.TableUtils.convertSelectExpressionItemIntoExpressions;
import static edu.buffalo.cse562.utils.TableUtils.toUnescapedString;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.queryplan.BufferDataSource;
import edu.buffalo.cse562.queryplan.DataSource;
import edu.buffalo.cse562.queryplan.FileDataSource;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class CartesianProduct {
	private Node node1;
	private Node node2;
	private Expression expression;
	
	
	public CartesianProduct(Node node1, Node node2,
			Expression expression) {
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
		DataSource dataFile1 = relationNode1.getFile();
		DataSource dataFile2 = relationNode2.getFile();
		List<Expression> table1ItemsExpression = convertSelectExpressionItemIntoExpressions(items);
		//dataFile1 = optimizeRelationNode(table1, dataFile1);
		//dataFile2 = optimizeRelationNode(table2, dataFile2);
		List<ColumnDefinition> newList = new ArrayList<ColumnDefinition>();
		newList.addAll(table1.getColumnDefinitions());
		newList.addAll(table2.getColumnDefinitions());
		CreateTable joinedTable = new CreateTable();
		String newTableName = getNewTableName(table1, table2);
		joinedTable.setTable(new Table(null, newTableName));
		joinedTable.setColumnDefinitions(newList);
		DataSourceSqlIterator sqlIterator1 = new DataSourceSqlIterator(table1,table1ItemsExpression , dataFile1,null,relationNode1.getExpression());
		LeafValue[] colVals1, colVals2;
		DataSource file = null;
		if(TableUtils.isSwapOn)
			file =new FileDataSource( new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat"));
		else
			file = new BufferDataSource();
		try {
			
			PrintWriter pw = new PrintWriter(file.getWriter());
			while((colVals1 = sqlIterator1.next()) != null) {
				DataSourceSqlIterator sqlIterator2 = new DataSourceSqlIterator(table2,convertSelectExpressionItemIntoExpressions( TableUtils.convertColumnDefinitionIntoSelectExpressionItems(table2.getColumnDefinitions())), dataFile2,null,relationNode2.getExpression());
				while((colVals2 = sqlIterator2.next()) != null) {
					List<LeafValue> fusedColumnValsList = new ArrayList<LeafValue>();
					fusedColumnValsList.addAll(Arrays.asList(colVals1));
					LeafValue[] fusedColsVals = new LeafValue[colVals1.length+colVals2.length];
					fusedColumnValsList.addAll(Arrays.asList(colVals2));
					fusedColumnValsList.toArray(fusedColsVals);
					String[] fusedColsValsString = new String[fusedColsVals.length]; 
					for(int j=0;j<fusedColsValsString.length;j++){
						fusedColsValsString[j]=toUnescapedString(fusedColsVals[j]);
					}
					if(expression!=null){
						ExpressionEvaluator evaluate = new ExpressionEvaluator(joinedTable);
						LeafValue leafValue = evaluate.evaluateExpression(expression, fusedColsValsString, null);
						BooleanValue value =(BooleanValue) leafValue;
						if(value ==BooleanValue.FALSE)
							continue;
					}
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
		} catch (IOException e) {
			throw new RuntimeException(e);
		}catch (SQLException e) {
			throw new RuntimeException(e);
		}
		sqlIterator1.close();
		//TableUtils.getTableSchemaMap().put(newTableName, newTable);
		RelationNode relationNode = new RelationNode(newTableName, null,file,joinedTable);
		return relationNode;
	}
	
	private DataSource optimizeRelationNode(CreateTable table, DataSource dataFile) {
		List <Expression> expressionList = TableUtils.expressionList;
		List<ColumnDefinition> colDefList = table.getColumnDefinitions();
		if (expressionList == null || expressionList.isEmpty())
			return dataFile;
		DataSource file =null;
		if(TableUtils.isSwapOn)
			file= new FileDataSource(new File(TableUtils.getTempDataDir() + File.separator + table.getTable().getName() 
				+ "_opt" + ".dat"));
		else
			file = new BufferDataSource();
		try {
			List <Expression> expList = new ArrayList <>();
			for (Expression exp : expressionList) {
				Expression exprLeft = ((BinaryExpression)exp).getLeftExpression();
				Expression exprRight = ((BinaryExpression)exp).getRightExpression();
				for (ColumnDefinition colDef : colDefList) {
					if ((exprLeft instanceof Column &&  colDef.getColumnName().equalsIgnoreCase(exprLeft.toString()))
						|| exprLeft instanceof LeafValue || exprLeft instanceof Function) {
						if ((exprRight instanceof Column &&  colDef.getColumnName().equalsIgnoreCase(exprRight.toString()))
								|| exprRight instanceof LeafValue || exprRight instanceof Function) {
								expList.add(exp);
								break;
						}
					}
				}
			}
			BufferedReader reader = new BufferedReader(dataFile.getReader());
			String line = "";
			ExpressionEvaluator evaluate = new ExpressionEvaluator(table);
			PrintWriter pw = new PrintWriter(file.getWriter());
			while ((line = reader.readLine()) != null) {
				String[] colVals = line.trim().split("\\|");
				boolean flag = true; 
				for (Expression expr : expList) {
					LeafValue leafValue = evaluate.evaluateExpression(expr, colVals, null);
					if (((BooleanValue)leafValue).getValue() == (Boolean.FALSE)) {
						flag = false;
						break;
					}					
				}
				if (flag) {
					pw.println(line);
				}
			}
			pw.close();
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			throw new RuntimeException("file not found ", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("IOException : ", e);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("SQLException : ", e);
		}
		return file;
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
