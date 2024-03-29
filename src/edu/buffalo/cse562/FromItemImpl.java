package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.datasource.FileDataSource;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;
import edu.buffalo.cse562.queryplan.QueryDomain;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class FromItemImpl implements FromItemVisitor {

	private Node node;
	private List<Table> tableList = new ArrayList<>();
	private List<String> tableNames = new ArrayList<>();
	private HashMap<String, RelationNode> tableToNodeMap = new HashMap<>();
	private QueryDomain queryDomain;

	public FromItemImpl(QueryDomain queryDomain) {
		this.queryDomain = queryDomain;
	}

	public HashMap<String, RelationNode> getTableToNodeMap() {
		return tableToNodeMap;
	}

	public List<String> getTableNames() {
		return tableNames;
	}

	@Override
	public void visit(Table table) {
		CreateTable schema = TableUtils.getTableSchemaMap().get(table.getName().toUpperCase());
		if (table.getAlias() == null)
			table.setAlias(table.getName());
		// if
		// (TableUtils.tableIndexMetaData.containsKey(table.getName().toUpperCase()))
		// {
		// node = new RelationNode(table.getName(), table.getAlias(), new
		// BerekelyDBDataSource(table.getName().toUpperCase()), schema);
		// } else {
			node = new RelationNode(table.getName(), table.getAlias(), new FileDataSource(table.getName().toUpperCase()), schema);
			// node = new RelationNode(table.getName(), table.getAlias(), new
			// BerekelyDBDataSource(table.getName().toUpperCase()),
			// TableUtils.getTableSchemaMap().get(table.getName().toUpperCase()));
		// }
		tableToNodeMap.put(table.getName().toUpperCase(), (RelationNode) node);
		tableList.add(table);
		tableNames.add(table.getAlias().toUpperCase());
	}

	@Override
	public void visit(SubSelect subselect) {
		// TODO Auto-generated method stub
		SelectVisitorImpl selectVistor = new SelectVisitorImpl();
		subselect.getSelectBody().accept(selectVistor);
		ProjectNode tempNode = (ProjectNode) selectVistor.getQueryPlanTreeRoot();
		if (subselect.getAlias() == null || subselect.getAlias().isEmpty()) {
			subselect.setAlias("temp");
		}
		List<SelectExpressionItem> subSelectItems = selectVistor.getSelectExpressionItems();
		tempNode.setPreferredAliasName(subselect.getAlias());
		// tableList.add(subselect.getAlias());
		Map<String, String> higherDomainColumnMap = queryDomain.getColumnTableMap();
		for (SelectExpressionItem item : subSelectItems) {
			String columnName;
			if (item.getExpression() instanceof Column) {
				columnName = ((Column) item.getExpression()).getColumnName();
			} else {
				// Since any expression other than Column has to have an alias
				// for resolution
				columnName = item.getAlias();
			}
			higherDomainColumnMap.put(columnName.toUpperCase(), subselect.getAlias().toUpperCase());
		}
		tableNames.add(subselect.getAlias().toUpperCase());
		// queryDomain.getQueryDomainTableSchemaMap().put(subselect.getAlias(),
		// tempNode.evalSchema());
		node = tempNode;
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Subjoin not supported");
	}

	public List<Table> getTableList() {
		return tableList;
	}

	public Node getFromItemNode() {
		return node;
	}
}
