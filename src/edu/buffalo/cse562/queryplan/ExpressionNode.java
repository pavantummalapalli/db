package edu.buffalo.cse562.queryplan;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.DataSourceSqlIterator;
import edu.buffalo.cse562.ExpressionEvaluator;
import edu.buffalo.cse562.datasource.BufferDataSource;
import edu.buffalo.cse562.datasource.DataSource;
import edu.buffalo.cse562.datasource.DataSourceReader;
import edu.buffalo.cse562.datasource.DataSourceWriter;
import edu.buffalo.cse562.datasource.FileDataSource;
import edu.buffalo.cse562.utils.TableUtils;

public class ExpressionNode implements Node {

	private Node parentNode;
	private Expression expression;
	private Node childNode;
	private boolean expressionDead;

	public ExpressionNode(Expression expression) {
		this.expression = expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	public boolean isExpressionDead() {
		return expressionDead;
	}
	
	public void setExpressionDead(boolean expressionDead) {
		this.expressionDead = expressionDead;
	}

	@Override
	public RelationNode eval() {
		try {
		RelationNode relationNode = childNode.eval();
		String tableName = relationNode.getTableName();
		CreateTable table = relationNode.getTable();
		DataSource dataFile = relationNode.getFile();
		DataSourceReader dataFileReader = dataFile.getReader(); 
		// TODO decide the table name convention
		if (!expressionDead) {
			String newTableName = tableName + "_new";
			DataSource file;
			LeafValue[] colVals;
			DataSourceSqlIterator sqlIterator = new DataSourceSqlIterator(
					table, null, dataFileReader, null, null);
			
			if (TableUtils.isSwapOn)
				file = new FileDataSource(new File(TableUtils.getTempDataDir()
						+ File.separator + newTableName + ".dat"),table.getColumnDefinitions());
			else
				file = new BufferDataSource();
			
				DataSourceWriter fileWriter = file.getWriter();
				ExpressionEvaluator evaluate = new ExpressionEvaluator(table);
				while ((colVals = sqlIterator.next()) != null) {
					int i;
//					String[] colValsString = new String[colVals.length];
//					for (int j = 0; j < colVals.length; j++) {
//						colValsString[j] = toUnescapedString(colVals[j]);
//					}
					LeafValue leafValue = evaluate.evaluateExpression(
							expression, colVals, null);
					BooleanValue value = (BooleanValue) leafValue;
					if (value == BooleanValue.FALSE)
						continue;
					fileWriter.writeNextTuple(colVals);
				}
				fileWriter.close();
			sqlIterator.close();
			dataFile.clear();
			relationNode.setFile(file);
			relationNode.setTableName(newTableName);
		}
		// file.renameTo(new File(TableUtils.getDataDir() + File.separator +
		// tableName + ".dat"));
		return relationNode;
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void setChildNode(Node childNode) {
		this.childNode = childNode;
		childNode.setParentNode(this);
	}

	public Node getChildNode() {
		return childNode;
	}

	@Override
	public CreateTable evalSchema() {
		return childNode.evalSchema();
	}

	public Expression getExpression() {
		return expression;
	}

	@Override
	public Node getParentNode() {
		return parentNode;
	}

	@Override
	public void setParentNode(Node parentNode) {
		this.parentNode = parentNode;
	}

	@Override
	public String toString() {
		// StringBuffer buffer = new StringBuffer(
		// "Apply Filter Expression : "+expression.toString());
		if(expressionDead)
			return "Expression Dead"; 
		StringBuffer buffer = new StringBuffer("Apply Filter Expression \n");
		buffer.append(childNode.toString() + "\n");
		return buffer.toString();
	}
}
