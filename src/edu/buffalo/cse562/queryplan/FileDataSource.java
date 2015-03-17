package edu.buffalo.cse562.queryplan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;

public class FileDataSource implements DataSource{

	private File file;
	private BufferedReader reader;
	private PrintWriter writer;
	
	public FileDataSource(File file) {
		this.file=file;
	}
	
	@Override
	public Reader getReader() throws IOException {
		reader =  new BufferedReader(new FileReader(file));
		return reader;
	}

	@Override
	public Writer getWriter() throws IOException {
		writer = new PrintWriter(file);
		return writer;
	}

	@Override
	public void close() throws IOException {
		if(reader!=null)
			reader.close();
		if(writer!=null)
			writer.close();
	}
}
