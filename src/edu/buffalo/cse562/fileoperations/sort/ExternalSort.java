package edu.buffalo.cse562.fileoperations.sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ExternalSort<V> {

	private Comparator<V> comparator;
	private Merger<V> merger;
	private Convertor<V> convertor;
	
	public ExternalSort(Comparator<V> comparator,Merger<V> merger,Convertor<V> convertor){
		this.comparator=comparator;
		this.merger=merger;
		this.convertor=convertor;
	}
	
	public static Long getAvailableMemory(){
		Long maxMem=Runtime.getRuntime().maxMemory();
		Long freeMem=Runtime.getRuntime().freeMemory();
		Long totMem=Runtime.getRuntime().totalMemory();
		Long totalFreeMem = maxMem - (totMem - freeMem);
		return totalFreeMem;
	}
	
	public static Long getAvailableMemoryInKB(){
		return getAvailableMemory()/1024;
	}
	
	public static Long getAvailableMemoryInMB(){
		return getAvailableMemoryInKB()/1024;
	}	
	
	public void externalSort(File[] sortedBlockFiles,File finalSortedFile){
		try{
		List<InnerDataBlock> blocks = new ArrayList<InnerDataBlock>(sortedBlockFiles.length);
		double eachBlockSize = ((0.8*getAvailableMemoryInKB().longValue())/(sortedBlockFiles.length+1));
		//Assuming each V type object size to be 4KB
		int threshold = Double.valueOf(eachBlockSize/(4.0)).intValue();  
		//System.out.println(threshold);
		for(int i=0;i<sortedBlockFiles.length;i++){
			InnerDataBlock tempBlock = new InnerDataBlock(sortedBlockFiles[i],threshold);
			if(tempBlock.size()>0)
				blocks.add(tempBlock);
		}
		mergeData(blocks,finalSortedFile,threshold);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	private void writeData(List<V> data,BufferedWriter stream) throws IOException{
		Iterator<V> ite = data.iterator();
		while(ite.hasNext()){	
			V mergedData = merger.writeMergedData(ite.next());
			stream.write(convertor.convertToString(mergedData)+"\n");
			//Do not remove this line. 
			//Free the contents once the block is written
			ite.remove();
		}
	}
	
	private void mergeData(List<InnerDataBlock> blocks,File finalSortedFile,int writeThreshold) throws FileNotFoundException, IOException, ClassNotFoundException{
		List<V> finalSortedList = new ArrayList<V>();
		BufferedWriter writeStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(finalSortedFile)));
		Integer sizeOfFinalArray = writeThreshold;
		while(blocks.size()>0){
			List<Integer> index = new ArrayList<Integer>();
			V minData = blocks.get(0).getFirstEntry();
			Set<V> mergedData=new HashSet<V>();
			mergedData.add(minData);
			index.add(0);
			//Iterate through blocks
			for(int i=1;i<blocks.size();i++){
				//if(minData>blocks.get(i).getFirstEntry()){
				if(compare(minData, blocks.get(i).getFirstEntry())>=1 ){
					minData = blocks.get(i).getFirstEntry();
					index= new ArrayList<Integer>();
					mergedData=new HashSet<V>();
					mergedData.add(minData);
					index.add(i);
				}
				else if(compare(minData, blocks.get(i).getFirstEntry())==0 ){
					//minData= mergeData(minData,blocks.get(i).getFirstEntry());
					mergedData.addAll(mergeData(minData,blocks.get(i).getFirstEntry()));
					index.add(i);
				}
			}
			//Copy the merged data to the final Array
			finalSortedList.addAll(mergedData);
			if(finalSortedList.size()>=sizeOfFinalArray){
				writeData(finalSortedList, writeStream);
			}
			//Increment the pointers
			incrementPointersAndDeleteFinishedBlocks(index,blocks);
		}
		writeData(finalSortedList, writeStream);
		writeStream.close();
	}
	
	private void incrementPointersAndDeleteFinishedBlocks(List<Integer> index,List<InnerDataBlock> dataBlocks) throws ClassNotFoundException, IOException{
		HashSet<InnerDataBlock> blocksToBeDeleted= new HashSet<InnerDataBlock>();
		for(int i=0;i<index.size();i++){
			InnerDataBlock block = dataBlocks.get(index.get(i).intValue());
			if(!block.popHeadData()){
				//flag this block from deletion
				blocksToBeDeleted.add(block);
			}
		}
		Iterator<InnerDataBlock> blocks= blocksToBeDeleted.iterator();
		while(blocks.hasNext()){
			dataBlocks.remove(blocks.next());
		}
	}
	
	public class InnerDataBlock {
		
		private final File associatedSortedFile;
		private List<V> block;
		boolean eof;
		private BufferedReader stream;
		private int threshold;
		
		public int getThreshold() {
			return threshold;
		}
		
		public int size(){
			return block.size();
		}
		
		
		public InnerDataBlock(File file,int threshold){
			try {
				this.threshold = threshold;
				block= new ArrayList<V>(threshold);
				this.associatedSortedFile=file;
				stream = new BufferedReader(new InputStreamReader(new FileInputStream(associatedSortedFile)));
				//Load data initially
				fetchData();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		public V getFirstEntry(){
			return block.get(0);
		}
		
		private boolean checkIfBlockisEmpty(){
			return block.size()==0;
		}
		
		public boolean popHeadData(){
			try{
				boolean dataAvailable;
				block.remove(0);
				//Check if block is empty
				if(checkIfBlockisEmpty()){
					//If the associated file is also empty
					if(eof){
						dataAvailable=false;
					}
					else{
						fetchData();
						//Check if the size increased after fetching data
						if(block.size()==0){
							dataAvailable=false;
							}
						else
							dataAvailable=true;
					}
				}
				else
					dataAvailable= true;
				return dataAvailable;
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		//Assuming the fact that fetchData will always be true when called for the first time through the DataBlock constructor method
		private void fetchData() throws ClassNotFoundException, IOException {
			V data;
			int count = threshold;
			while(count!=0){
				String dataInStringForm = stream.readLine();
				if(dataInStringForm==null){
					stream.close();
					associatedSortedFile.delete();
					eof =true;
					return ;
				}
				data=convertor.parseFromString(dataInStringForm);
				block.add(data);
				count--;
			}
			eof= false;
		}
	}

	private List<V> mergeData(V object1, V object2) {
		return merger.mergeData(object1, object2);
	}

	private int compare(V o1, V o2) {
		return comparator.compare(o1, o2);
	}
}
