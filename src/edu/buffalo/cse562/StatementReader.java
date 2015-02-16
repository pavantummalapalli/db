package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class StatementReader {

	public void readSqlFile(String dataDir, String[] sqlfiles) {
		try {
			for (int i=0;i<sqlfiles.length;i++) {
				File file = new File(dataDir + "/" + sqlfiles[i]);
				CCJSqlParser parser = new CCJSqlParser(new FileReader(file));
				Statement statement;
				while ((statement = parser.Statement()) != null) {
					if (statement instanceof Select) {
						SelectHandler<Void> handler = new SelectHandler<Void>() {

							@Override
							public Void processStatement(Select selectStatement) {
								SelectQueryEvaluator selectVisitor = new SelectQueryEvaluator(dataDir);
								selectStatement.getSelectBody().accept(selectVisitor);
								return null;
							}
						};
						handler.processStatement((Select) statement);
					} else if (statement instanceof CreateTable) {
						CreateTableHandler<Void> handler = new CreateTableHandler<Void>() {

							@Override
							public Void processStatement(
									CreateTable createTableStatement) {
								// TODO Auto-generated method stub
								return null;
							}
						};
						handler.processStatement((CreateTable) statement);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
