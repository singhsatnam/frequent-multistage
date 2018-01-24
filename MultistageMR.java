package wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import wordcount.CandidateCreator;

public class MultistageMR {
	public static int threshold;
	private static LinkedHashMap<ArrayList<Integer>, Integer> freqMap = new LinkedHashMap<>();
	private static HashMap<ArrayList<Integer>, Integer> candidateMap = new HashMap<ArrayList<Integer>, Integer>();
	private static boolean changeInList = false;
	private static final int ITEM_RANGE = 100;
	private static final String FILENAME = "filename.txt";

	

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		//
		// private String[] getItemSetList(String line)
		// {
		// String[] itemList = {};
		// return itemList;
		// }

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		   String line = value.toString();
//		   System.out.println("first line: " + line);
		   line = line.substring(line.indexOf(',') + 1);
		   String[] strArr = line.split(",");
	
		   
//		   System.out.print(candidateMap.keySet() + "\n");
//		   System.out.print(lastFrequentList + "\n");
		   for(ArrayList<Integer> itemSet : candidateMap.keySet()){
			   boolean flag = true;
			   for(int item : itemSet)
			   {
				   if(Arrays.asList(strArr).contains(Integer.toString(item))){
					   continue;
				   }
				   else
				   {
					   flag = false;
					   break;
				   }
			   }
			   if(flag){
				   
					String strKey = itemSet.toString();
				   word.set(strKey.substring(1, strKey.length()-1));
//				   System.out.print("\n\t Sending to reducer "+ word.toString());
				   output.collect(word, one);
			   }
			 
		   }
		  
		 }
	
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

//		public enum UpdateCounter {
//			UPDATED
//		}

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				int a = values.next().get();
//				System.out.println(key.toString() + "_Value: " + a);
				sum += a;
			}
//			System.out.println("t: " + threshold);
			if (sum >= threshold) {
				output.collect(key, new IntWritable(sum));
				String[] strArr = key.toString().split(",");
//				System.out.println("\\\\\\\\\\\\\\\\\\\\\tReduce print:" + key.toString());
				ArrayList<Integer> intArr = new ArrayList<>();
				for(String item : strArr){
					intArr.add(Integer.parseInt(item.trim()));
				}
				freqMap.put(intArr, sum);
//				System.out.println("lastFrequenList in reducer: " + freqMap.keySet());
				changeInList = true;
			}
			
				
		}
		
		
	}
	
	public static void getThreshold(String fileName)throws IOException
	   {
		   fileName = fileName + "input.txt";
			    RandomAccessFile raf = new RandomAccessFile(fileName, "rw");          
			     //Initial write position                                             
			    long writePosition = raf.getFilePointer();                            
			    threshold = Integer.parseInt(raf.readLine());                                                       
			    // Shift the next lines upwards.                                      
			    long readPosition = raf.getFilePointer();                             

			    byte[] buff = new byte[1024];                                         
			    int n;                                                                
			    while (-1 != (n = raf.read(buff))) {                                  
			        raf.seek(writePosition);                                          
			        raf.write(buff, 0, n);                                            
			        readPosition += n;                                                
			        writePosition += n;                                               
			        raf.seek(readPosition);                                           
			    }                                                                     
			    raf.setLength(writePosition);                                         
			    raf.close();                                                          
			   
	   }

//	public static File lastFileModified(String dir) {
//		File fl = new File(dir);
//		File[] files = fl.listFiles(new FileFilter() {
//			public boolean accept(File file) {
//				return file.isFile();
//			}
//		});
//		long lastMod = Long.MIN_VALUE;
//		File choice = null;
//		for (File file : files) {
//			if (file.lastModified() > lastMod) {
//				choice = file;
//				lastMod = file.lastModified();
//			}
//		}
//		return choice;
//	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		getThreshold(args[0]);
		System.out.println("Support Threshold: " + threshold);
		CandidateCreator.setItemRange(ITEM_RANGE);
	   candidateMap.clear();
	   candidateMap = CandidateCreator.createCandidateMap(new ArrayList<>(freqMap.keySet()));
	   //freqMap.clear();

//		System.out.println(lastFileModified(args[0]).toString());
		// Configuration conf = new Configuration();
		// Job job = new Job(conf);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("MultistageMR");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		RunningJob some_job;

		changeInList = true;
//		int depth = 0;
//		depth++;
		//long counter = rjob.getCounters().findCounter(Reduce.UpdateCounter.UPDATED).getValue();
//		BufferedReader br = new BufferedReader(new FileReader(args[1] + lastFileModified(args[1]).toString().split("/")[1] ));
		while (!candidateMap.isEmpty() && changeInList) 
		{// check if file is empty instead of
			
			changeInList = false;
//			System.out.println("depth:start "+ depth);
//			System.out.println("Last Frequent List: " + lastFrequentList);
			
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(out))
				fs.delete(out, true);
		   
			some_job = JobClient.runJob(conf);
//			some_job.waitForCompletion();
			
//			conf = new JobConf(WordCount.class);
//			conf.set("recursion.depth", depth + "");
//			conf.setJobName("MultistageMR" + depth);
//			conf.setMapperClass(Map.class);
//			conf.setCombinerClass(Reduce.class);
//			conf.setReducerClass(Reduce.class);
			
//			SequenceFileInputFormat.addInputPath(conf, in);

//			conf.setOutputKeyClass(Text.class);
//			conf.setOutputValueClass(IntWritable.class);
//			conf.setInputFormat(TextInputFormat.class);
//			conf.setOutputFormat(TextOutputFormat.class);
//			SequenceFileOutputFormat.setOutputPath(conf, out);
			// wait for completion and update the counter
//		   System.out.println("Candidate Map: " + candidateMap);
		   candidateMap.clear();
		   candidateMap = CandidateCreator.createCandidateMap(new ArrayList<>(freqMap.keySet()));
		   System.out.println("Last Frequent List: " + freqMap.keySet());
		   //append to output(lastFrequentList);
		   
		   try {
				
				File file = new File(FILENAME);
				// if file does not exists, then create it
				if (!file.exists()) {
					file.createNewFile();
				}								
				FileWriter fileW = new FileWriter(file.getAbsoluteFile(), true);
				BufferedWriter 	bf= new BufferedWriter(fileW);
				bf.write(freqMap+"\n");
				bf.flush();
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		   freqMap.clear();
		   
//			depth++;
//			System.out.println("depth:end "+ depth);
		}
		//write to an existing final output file/create file if not existing

		
		
	}

}
