package edu.buffalo.cse562.datasource;

import static edu.buffalo.cse562.utils.TableUtils.toUnescapedString;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.utils.TableUtils;

public class FileDataSource implements DataSource,DataSourceReader,DataSourceWriter{

	private File file;
	private BufferedReader reader;
	private PrintWriter writer;
	private HashMap<String, Integer> columnMapping=new HashMap<>();
	private HashMap<Integer,String> reverseColumnMapping=new HashMap<>();
	private List<ColumnDefinition> colDefns;
	private String tableName;
	private boolean init;
	
	public FileDataSource(String tableName) {
		this.tableName = tableName.toUpperCase();
		this.file = TableUtils.getAssociatedTableFile(tableName);
	}

	private void init() {
		this.file = TableUtils.getAssociatedTableFile(tableName);
		this.colDefns = TableUtils.getTableSchemaMap().get(tableName.toUpperCase()).getColumnDefinitions();
		Iterator<ColumnDefinition> iterator = colDefns.iterator();
		int index = 0;
		while(iterator.hasNext()) {
			ColumnDefinition cd = iterator.next();
			columnMapping.put(cd.getColumnName(), index);
			reverseColumnMapping.put(index, cd.getColumnName());
			index++;
		}
		init = true;
	}
	
	public FileDataSource(File file, List<ColumnDefinition> colDefns) {
		this.colDefns = TableUtils.getTableSchemaMap().get(tableName.toUpperCase()).getColumnDefinitions();
		Iterator<ColumnDefinition> iterator = colDefns.iterator();
		int index = 0;
		while (iterator.hasNext()) {
			ColumnDefinition cd = iterator.next();
			columnMapping.put(cd.getColumnName(), index);
			reverseColumnMapping.put(index, cd.getColumnName());
			index++;
		}
	}

	@Override
	public void close() throws IOException {
		if(reader!=null)
			reader.close();
		if(writer!=null)
			writer.close();
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	@Override
	public LeafValue[] readNextTuple() throws IOException {
		if (!init)
			init();
		String row = reader.readLine();
		if(row==null || row.isEmpty())
			return null;
		// StringTokenizer tokenizer = new StringTokenizer(row, "|");
		// int count = tokenizer.countTokens();
		// String[] colVals = new String[count];
		// for(int i=0;i<count;i++)
		// colVals[i]=tokenizer.nextToken();
		String[] colVals = TableUtils.pattern.split(row);
		LeafValue[] convertedValues = null;
		Map<String, Integer> physicalColumnMap = TableUtils.physicalColumnMapping.get(tableName);
		if (physicalColumnMap != null) {
			String[] newColVals = new String[columnMapping.size()];
			convertedValues = new LeafValue[newColVals.length];
			Iterator<String> physicalColumnMapIte = physicalColumnMap.keySet().iterator();
			while (physicalColumnMapIte.hasNext()) {
				String columnName = physicalColumnMapIte.next();
				if (columnMapping.get(columnName) != null)
				newColVals[columnMapping.get(columnName)] = colVals[physicalColumnMap.get(columnName)];
			}
			for (int i = 0; i < newColVals.length; i++) {
				String columnName = reverseColumnMapping.get(i);
				convertedValues[i] = TableUtils.getLeafValue(columnName, columnMapping, newColVals, colDefns);
			}
		} else {
			convertedValues = new LeafValue[colVals.length];
			for (int i = 0; i < colVals.length; i++) {
				String columnName = reverseColumnMapping.get(i);
				convertedValues[i] = TableUtils.getLeafValue(columnName, columnMapping, colVals, colDefns);
			}
		}
		return convertedValues;
	}

	@Override
	public void writeNextTuple(LeafValue[] tuple) throws IOException {
		int i=1;
		for(;i<tuple.length; i++) {
			writer.print(toUnescapedString(tuple[i-1]) + "|");
		}
		if(tuple.length > 0)
			writer.println(toUnescapedString(tuple[i-1]));
	}

	@Override
	public DataSourceReader getReader() throws IOException {
		close();
		reader =  new BufferedReader(new FileReader(file));
		return this;
	}

	@Override
	public DataSourceWriter getWriter() throws IOException {
		close();
		writer = new PrintWriter(file);
		return this;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
	}

	@Override
	public long getEstimatedDataSourceSize() {
		return file.length();
	}
}
