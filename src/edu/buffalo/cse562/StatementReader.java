package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;
import edu.buffalo.cse562.queryplan.QueryOptimizer;
import edu.buffalo.cse562.utils.TableUtils;

public class StatementReader {

	String query = "";
	public void readSqlFile(String dataDir, String[] sqlfiles) {
		try {
			for (int i=0;i<sqlfiles.length;i++) {
				File file = new File(sqlfiles[i]);
				CCJSqlParser parser = new CCJSqlParser(new FileReader(file));
				Statement statement;
				
				while ((statement = parser.Statement()) != null) {
					if (statement instanceof Select) {
						query = statement.toString();
						SelectVisitorImpl selectVistor=new SelectVisitorImpl();
						((Select)statement).getSelectBody().accept(selectVistor);
						Node node = selectVistor.getQueryPlanTreeRoot();
						node=new QueryOptimizer().optimizeQueryPlan((ProjectNode)node);
						node.eval();
					} else if (statement instanceof CreateTable) {
						try {
							CreateTable createTableStmt = (CreateTable) statement;
							String tableName = createTableStmt.getTable().getName();
							TableUtils.getTableSchemaMap().put(tableName.toUpperCase(), createTableStmt);
						} catch (Exception ex) {	
							throw new RuntimeException("CREATE TABLE THROW NEW EXCEPTION : " + statement, ex);
						}
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Runtime Exception at StatementReader for query : " + query , e);
		}
	}
}
