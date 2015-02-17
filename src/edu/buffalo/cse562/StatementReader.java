package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.utils.TableUtils;

public class StatementReader {

	public void readSqlFile(String dataDir, String[] sqlfiles) {
		try {
			//Remove next 2 lines. Just for my local use
			File dir = new File(dataDir);
			sqlfiles = dir.list();
			for (int i=0;i<sqlfiles.length;i++) {
				File file = new File(sqlfiles[i]);
				CCJSqlParser parser = new CCJSqlParser(new FileReader(file));
				Statement statement;
				while ((statement = parser.Statement()) != null) {
					if (statement instanceof Select) {
//						SelectQueryEvaluator selectVisitor = new SelectQueryEvaluator(dataDir);
						SelectVisitorImpl selectVistor=new SelectVisitorImpl();
						((Select)statement).getSelectBody().accept(selectVistor);
						Node node = selectVistor.getQueryPlanTreeRoot();
						//TODO evaluation of the query plan
					} else if (statement instanceof CreateTable) {
						CreateTable createTableStmt = (CreateTable) statement;
						String tableName = createTableStmt.getTable().getName();
						TableUtils.getTableSchemaMap().put(tableName, createTableStmt);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
