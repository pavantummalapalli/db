package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import edu.buffalo.cse562.berkelydb.CreateTableIndex;
import edu.buffalo.cse562.berkelydb.DatabaseManager;
import edu.buffalo.cse562.berkelydb.InitializeTableIndexMetaData;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;
import edu.buffalo.cse562.queryplan.QueryOptimizer;
import edu.buffalo.cse562.utils.TableUtils;

public class StatementReader {

	String query = "";
	public void readSqlFile(String dataDir, String[] sqlfiles) {
		try {
			long startTime = System.currentTimeMillis();
			DatabaseManager manager = new DatabaseManager(TableUtils.getDbDir());
			System.out.println("Time in seconds to initt :  " + (System.currentTimeMillis() - startTime) / 1000.0);
			startTime = System.currentTimeMillis();
			for (int i=0;i<sqlfiles.length;i++) {
				File file = new File(sqlfiles[i]);
				CCJSqlParser parser = new CCJSqlParser(new FileReader(file));
				Statement statement;
				
				while ((statement = parser.Statement()) != null) {
					if (statement instanceof Select) {
						long startQuery = System.currentTimeMillis();
						query = statement.toString();
						SelectVisitorImpl selectVistor=new SelectVisitorImpl();
						((Select)statement).getSelectBody().accept(selectVistor);
						Node node = selectVistor.getQueryPlanTreeRoot();
						node=new QueryOptimizer().optimizeQueryPlan((ProjectNode)node);
						node.eval();
						System.out.println(System.currentTimeMillis() - startQuery);
						// manager.publishStats();
						// manager.printCacheMisses();
					} else if (statement instanceof CreateTable) {
						try {
							CreateTable createTableStmt = (CreateTable) statement;
							Table table = createTableStmt.getTable();
							String tableName = table.getName();
							TableUtils.getTableSchemaMap().put(table.getName(), createTableStmt);
                            if (TableUtils.isLoadPhase) {
                                CreateTableIndex createTableIndex = new CreateTableIndex(createTableStmt,manager);
                                createTableIndex.createIndexForTable();
                            } else {
								// if
								// (TableUtils.tableIndexMetaData.containsKey(tableName.toUpperCase())
								// == false &&
								// !table.getName().toUpperCase().equals("LINEITEM"))
								// {
								if (TableUtils.tableIndexMetaData.containsKey(tableName.toUpperCase()) == false) {
                                    InitializeTableIndexMetaData init = new InitializeTableIndexMetaData(createTableStmt,manager);
                                    init.initializeIndexMetaData();
                                }
                            }
						} catch (Exception ex) {	
							throw new RuntimeException("CREATE TABLE THROW NEW EXCEPTION : " + statement, ex);
						}
					}
				}
			}
			manager.close();
			long endTime = System.currentTimeMillis();
			System.out.println("Time in seconds " + (endTime - startTime) / 1000.0);
		} catch (Throwable e) {
			throw new RuntimeException("Runtime Exception at StatementReader for query : " + query , e);
		}
	}	
}
