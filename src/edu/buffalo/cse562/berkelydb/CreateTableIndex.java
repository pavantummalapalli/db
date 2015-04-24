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
    private Database primaryDatabase;
    private DatabaseManager manager;

    public CreateTableIndex(CreateTable createTable,DatabaseManager manager) {
    	this.manager = manager;
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
            TupleBinding<LeafValue[]> tupleBinding = TableUtils.getTupleBindingForTable(tableName);
            System.out.println("building index for :"+tableName);
            primaryDatabase = manager.createIndexedTable(tableName, primaryKeyIndex, file, tupleBinding, colDefns);
        }
    }

    private void createSecondaryIndex() {
        List<Integer> secondaryKeyIndexList = TableUtils.tableNameSecondaryIndexMap.get(tableName);
        if (secondaryKeyIndexList != null) {
            File file = TableUtils.getAssociatedTableFile(tableName);
            TupleBinding<LeafValue[]> tupleBinding = TableUtils.getTupleBindingForTable(tableName);
            for (int secondaryKeyIndex : secondaryKeyIndexList) {
                SecondaryKeyCreaterImpl secondaryKeyCreater = new SecondaryKeyCreaterImpl(tupleBinding, secondaryKeyIndex);
                String secondaryIndexName = tableName + "." + ((ColumnDefinition)createTable.getColumnDefinitions().get(secondaryKeyIndex)).getColumnName().toUpperCase();
                System.out.println("building index for :"+tableName);
                manager.createSecondaryIndexedTable(primaryDatabase, secondaryIndexName, secondaryKeyCreater);
            }
        }
    }
}
