package edu.buffalo.cse562;

import edu.buffalo.cse562.datasource.*;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static edu.buffalo.cse562.utils.TableUtils.convertColumnDefinitionIntoSelectExpressionItems;
import static edu.buffalo.cse562.utils.TableUtils.convertSelectExpressionItemIntoExpressions;

/**
 * Created by cksharma on 4/28/15.
 */
public class IndexLoopJoin {
    private Node node1;
    private Node node2;
    private Expression expression;


    public IndexLoopJoin(Node node1, Node node2, Expression expression) {
        this.node1 = node1;
        this.node2 = node2;
        this.expression = expression;
    }

    public RelationNode doIndexLoopJoin() {
        try {
            RelationNode relationNode1 = node1.eval();
            RelationNode relationNode2 = node2.eval();
            CreateTable table1 = relationNode1.getTable();
            CreateTable table2 = relationNode2.getTable();
            List<SelectExpressionItem> items = convertColumnDefinitionIntoSelectExpressionItems(table1.getColumnDefinitions());
            DataSource dataFile1 = relationNode1.getFile();
            DataSource dataFile2 = relationNode2.getFile();
            List<Expression> table1ItemsExpression = convertSelectExpressionItemIntoExpressions(items);
            //dataFile1 = optimizeRelationNode(table1, dataFile1);
            //dataFile2 = optimizeRelationNode(table2, dataFile2);
            List<ColumnDefinition> newList = new ArrayList<ColumnDefinition>();
            newList.addAll(table1.getColumnDefinitions());
            newList.addAll(table2.getColumnDefinitions());
            CreateTable joinedTable = new CreateTable();
            String newTableName = getNewTableName(table1, table2);
            joinedTable.setTable(new Table(null, newTableName));
            joinedTable.setColumnDefinitions(newList);
            DataSourceSqlIterator sqlIterator1 = new DataSourceSqlIterator(table1,table1ItemsExpression , dataFile1.getReader(),null,relationNode1.getExpression());
            LeafValue[] colVals1;
            DataSource file = null;
            if(TableUtils.isSwapOn)
                file =new FileDataSource( new File(TableUtils.getTempDataDir() + File.separator + newTableName + ".dat"),newList);
            else
                file = new BufferDataSource();

            int index = 0;
            Column column1 = (Column)((BinaryExpression)expression).getLeftExpression();
            for (Object columnDefinition : relationNode1.getTable().getColumnDefinitions()) {
                if (((ColumnDefinition)columnDefinition).getColumnName().equals(column1.getColumnName())) {
                    break;
                }
                ++index;
            }

            DataSourceWriter fileWriter = file.getWriter();

            while((colVals1 = sqlIterator1.next()) != null) {
                LeafValue leafValue = colVals1[index];
                LeafValue[] colVals2 = ((BerekelyDBDataSource) ((RelationNode) node2).getFile()).lookupPrimaryIndex(leafValue);
                if (colVals2 == null)
                    continue;
                List<LeafValue> fusedColumnValsList = new ArrayList<LeafValue>();
                fusedColumnValsList.addAll(Arrays.asList(colVals1));
                LeafValue[] fusedColsVals = new LeafValue[colVals1.length + colVals2.length];
                fusedColumnValsList.addAll(Arrays.asList(colVals2));
                fusedColumnValsList.toArray(fusedColsVals);

                int z=0;
                LeafValue[] joinedTuple = new LeafValue[colVals1.length+colVals2.length];
                for(int i=0; i<colVals1.length; i++,z++) {
                    joinedTuple[z]=colVals1[i];
                }
                for(int i=0; i<colVals2.length; i++,z++) {
                    joinedTuple[z]=colVals2[i];
                }
                fileWriter.writeNextTuple(joinedTuple);
            }

            fileWriter.close();
            sqlIterator1.close();
            dataFile1.clear();
            dataFile2.clear();
            RelationNode relationNode = new RelationNode(newTableName, null,file,joinedTable);
            return relationNode;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getNewTableName(CreateTable table1,CreateTable table2){
        return table1.getTable().getName() + "x" + table2.getTable().getName();
    }
}
