package edu.buffalo.cse562;

import edu.buffalo.cse562.berkelydb.CreateTableIndex;
import edu.buffalo.cse562.berkelydb.InitializeTableIndexMetaData;
import edu.buffalo.cse562.memmanage.Listener;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;
import edu.buffalo.cse562.queryplan.QueryOptimizer;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import javax.management.NotificationEmitter;
import java.io.File;
import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.Collection;

public class StatementReader {

	String query = "";
	public void readSqlFile(String dataDir, String[] sqlfiles) {
		try {
			long startTime = System.currentTimeMillis();
			//attachMemoryListeners();
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
							Table table = createTableStmt.getTable();
							String tableName = table.getName();
							TableUtils.getTableSchemaMap().put(table.getName(), createTableStmt);
//							if(table.getAlias()==null)
//								table.setAlias(createTableStmt.getTable().getName());
//							TableUtils.getColumnTableMap(createTableStmt.getColumnDefinitions(), table);
//							TableUtils.res

                            //Load Phase is On. Have to create primary and secondary indexes
                            if (TableUtils.isLoadPhase) {
                                CreateTableIndex createTableIndex = new CreateTableIndex(createTableStmt);
                                createTableIndex.createIndexForTable();
                            } else {
                                if (TableUtils.tableIndexMetaData.containsKey(tableName.toUpperCase()) == false) {
                                    InitializeTableIndexMetaData init = new InitializeTableIndexMetaData(createTableStmt);
                                    init.initializeIndexMetaData();
                                }
                            }
						} catch (Exception ex) {	
							throw new RuntimeException("CREATE TABLE THROW NEW EXCEPTION : " + statement, ex);
						}
					}
				}
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Time in seconds " + (endTime - startTime) / 1000.);
		} catch (Throwable e) {
			throw new RuntimeException("Runtime Exception at StatementReader for query : " + query , e);
		}
	}
	
	private void attachMemoryListeners(){
		MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
		Collection<MemoryPoolMXBean> pool = ManagementFactory.getMemoryPoolMXBeans();
		for(MemoryPoolMXBean temp:pool){
			if(temp.getType().compareTo(MemoryType.HEAP)==0){
				if(temp.isUsageThresholdSupported()){
					//Set at 80% usage
					temp.setUsageThreshold(new Double(temp.getUsage().getMax()*.6).longValue());
				}
			}
		}
		NotificationEmitter emitter = (NotificationEmitter) mbean;
		Listener listener = new Listener();
		emitter.addNotificationListener(listener, null, null);
	}
}
