package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
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
						SelectQueryEvaluator selectVisitor = new SelectQueryEvaluator(dataDir);
						((Select)statement).getSelectBody().accept(selectVisitor);
					} else if (statement instanceof CreateTable) {
						//TODO create appropriate visitor
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
