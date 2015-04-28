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
import java.util.StringTokenizer;

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
	
	public FileDataSource(File file,List<ColumnDefinition> colDefns) {
		this.file=file; 
		this.colDefns=colDefns;
		Iterator<ColumnDefinition> iterator = colDefns.iterator();
		int index = 0;
		while(iterator.hasNext()) {
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
		String row = reader.readLine();
		if(row==null || row.isEmpty())
			return null;
		StringTokenizer tokenizer = new StringTokenizer(row, "|");
		int count = tokenizer.countTokens();
		String[] colVals = new String[count];
		for(int i=0;i<count;i++)
			colVals[i]=tokenizer.nextToken();
		LeafValue [] convertedValues = new LeafValue[count];
		for(int i=0;i<colVals.length;i++){
			String columnName = reverseColumnMapping.get(i);
			convertedValues[i]=TableUtils.getLeafValue(columnName, columnMapping, colVals, colDefns);
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
