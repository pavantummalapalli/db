package edu.buffalo.cse562.queryplan;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

public class BufferDataSource implements DataSource {

	private StringBuffer buffer;
	private StringReader reader;
	private StringWriter writer;
	
	@Override
	public Reader getReader() throws IOException {
		if(writer!=null){
			addToBuffer();
		}
		reader = new StringReader(buffer.toString());
		return reader;
	}

	@Override
	public Writer getWriter() throws IOException {
		if(writer!=null){
			addToBuffer();
			writer.close();
		}
		writer = new StringWriter();
		return writer;
	}
	
	public void close() throws IOException {
		if(reader!=null)
			reader.close();
		if(writer!=null){
			addToBuffer();
			writer.close();
		}
	}
	
	private void addToBuffer(){
		buffer = writer.getBuffer();
	}
}
