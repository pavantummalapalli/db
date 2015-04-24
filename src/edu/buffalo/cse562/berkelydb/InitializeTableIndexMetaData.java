package edu.buffalo.cse562.berkelydb;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.SecondaryDatabase;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InitializeTableIndexMetaData {

    private CreateTable createTable;
    private String tableName;

    public InitializeTableIndexMetaData(CreateTable createTable) {
        this.createTable = createTable;
        this.tableName = this.createTable.getTable().getName().toUpperCase();
    }

    public void initializeIndexMetaData() {
        IndexMetaData indexMetaData = new IndexMetaData();
        DatabaseManager manager = new DatabaseManager(TableUtils.getDbDir());
        String primaryIndexName = tableName;
        Database primaryDatabase = manager.getPrimaryDatabase(tableName);
        TupleBinding<LeafValue[]> binding = TableUtils.getTupleBindingForTable(tableName);
        Map<String, SecondaryDatabase> secondaryDatabases = null;

        List<Integer> secondaryKeyIndexList = TableUtils.tableNameSecondaryIndexMap.get(tableName);
        if (secondaryKeyIndexList != null) {
            secondaryDatabases = new HashMap<>();
            for (int secondaryKeyIndex : secondaryKeyIndexList) {
                SecondaryKeyCreaterImpl secondaryKeyCreater = new SecondaryKeyCreaterImpl(binding, secondaryKeyIndex);
                String secondaryIndexName = tableName + "." + ((ColumnDefinition) createTable.getColumnDefinitions().get(secondaryKeyIndex)).getColumnName().toUpperCase();
                SecondaryDatabase secDb = manager.getSecondaryDatabase(primaryDatabase, secondaryIndexName, secondaryKeyCreater);
                secondaryDatabases.put(secondaryIndexName, secDb);
            }
        }

        indexMetaData.setPrimaryIndexName(primaryIndexName);
        indexMetaData.setPrimaryDatabase(primaryDatabase);
        indexMetaData.setBinding(binding);
        indexMetaData.setSecondaryIndexes(secondaryDatabases);

        TableUtils.tableIndexMetaData.put(tableName, indexMetaData);
    }
}
