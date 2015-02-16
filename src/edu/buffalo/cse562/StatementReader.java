package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;

import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class StatementReader {

	public void readSqlFile(String dataDir, String[] sqlfiles) {
		try {
			for (int i=0;i<sqlfiles.length;i++) {
				File file = new File(sqlfiles[i]);
				CCJSqlParser parser = new CCJSqlParser(new FileReader(file));
				Statement statement;
				while ((statement = parser.Statement()) != null) {
					if (statement instanceof Select) {
						SelectQueryEvaluator selectVisitor = new SelectQueryEvaluator(dataDir);
						((Select)statement).getSelectBody().accept(selectVisitor);
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
