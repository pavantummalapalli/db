package edu.buffalo.cse562.berkelydb;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.File;
import java.util.List;

public class CreateTableIndex {

    private CreateTable createTable;
    private String tableName;
    Database primaryDatabase;

    public CreateTableIndex(CreateTable createTable) {
        this.createTable = createTable;
        tableName = this.createTable.getTable().getName().toUpperCase();
    }

    public void createIndexForTable() {
        createIndexForPrimaryKey();
        createSecondaryIndex();
    }

    private void createIndexForPrimaryKey() {

        Integer primaryKeyIndex = TableUtils.tableNamePrimaryIndexMap.get(tableName);
        if (primaryKeyIndex != null) {
            File file = TableUtils.getAssociatedTableFile(tableName);
            List<ColumnDefinition> colDefns = createTable.getColumnDefinitions();
            DatabaseManager manager = new DatabaseManager(TableUtils.getDbDir());
            TupleBinding<LeafValue[]> tupleBinding = TableUtils.getTupleBindingForTable(tableName);
            primaryDatabase = manager.createIndexedTable(tableName, primaryKeyIndex, file, tupleBinding, colDefns);
            manager.close();
        }
    }

    private void createSecondaryIndex() {
        List<Integer> secondaryKeyIndexList = TableUtils.tableNameSecondaryIndexMap.get(tableName);
        if (secondaryKeyIndexList != null) {
            File file = TableUtils.getAssociatedTableFile(tableName);
            TupleBinding<LeafValue[]> tupleBinding = TableUtils.getTupleBindingForTable(tableName);
            DatabaseManager manager = new DatabaseManager(TableUtils.getDbDir());
            for (int secondaryKeyIndex : secondaryKeyIndexList) {
                SecondaryKeyCreaterImpl secondaryKeyCreater = new SecondaryKeyCreaterImpl(tupleBinding, secondaryKeyIndex);
                String secondaryIndexName = tableName + "." + ((ColumnDefinition)createTable.getColumnDefinitions().get(secondaryKeyIndex)).getColumnName().toUpperCase();
                manager.createSecondaryIndexedTable(primaryDatabase, secondaryIndexName, secondaryKeyCreater);
                manager.close();
            }
        }
    }

}
