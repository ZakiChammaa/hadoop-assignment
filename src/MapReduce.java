import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduce {
	public static class Mapper1 extends Mapper<Object, Text, Text, LongWritable> {
	
		private final static Text word = new Text();
	    private final static LongWritable one = new LongWritable(1);
	    private ArrayList<String> transactions = new ArrayList<String>();
	    
	    /**
	     * Count number of baskets in input file.
	     * 
	     * @param file:        Input file
	     * @return:            Number of lines
	     * @throws IOException
	     */
	    public int countBaskets(String file) throws IOException{
	    	int lineCount = 0;
	    	BufferedReader fileReader = new BufferedReader(new FileReader(file));
	    	try {
	    	    String line = fileReader.readLine();
	    	    
	    	    while (line != null) {
	    	        line = fileReader.readLine();
	    	        lineCount++;
	    	    }
	    	} finally {
	    		fileReader.close();
	    	}
	    	return lineCount;
	    }
	    
	    /**
	     * Phase 1 mapper function.
	     * Takes the input sub-file basket by basket and loads into an ArrayList.
	     * Once the ArrayList contains all the elements, we perform Apriori on it.
	     */
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	PreProcessInput p = new PreProcessInput();
	    	int localThreshold = p.getThreshold() / p.getNumberOfInputFiles();
	    	String currentFile = ((FileSplit) context.getInputSplit()).getPath().getName();
	    	int countLines = countBaskets(String.format("input/%s", currentFile));
	    	
			String line = value.toString();
			transactions.add(line);
			
			if(transactions.size() == countLines){
				String minsup = String.valueOf(localThreshold);
				ArrayList<ArrayList<String>> finalFrequentItemSet = new ArrayList<ArrayList<String>>();
				ArrayList<String> firstFrequentSet =  Apriori.findFirstLevelCandidateItemSets(minsup, transactions);
				
				ArrayList<String> newCandidateSet = Apriori.findKLevelCandidateItemSets(minsup, firstFrequentSet);
				while(newCandidateSet.size() != 0) {
					ArrayList<String> nextFrequentSet = Apriori.findFirstLevelCandidateItemSets(minsup, transactions, newCandidateSet);
					finalFrequentItemSet.add(nextFrequentSet);
					newCandidateSet = Apriori.findKLevelCandidateItemSets(minsup, nextFrequentSet);
				}
				
				for(ArrayList<String> frequentItemList : finalFrequentItemSet) {
					for(String itemSet : frequentItemList) {
						word.set(itemSet.trim());
						context.write(word, one);
					}
				}
			}
	    }
	}

	public static class Reducer1 extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		
		/**
		 * Reducer function that outputs the result from the mapper to the output folder.
		 */
	    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	    	result.set(1);
	    	context.write(key, result);
	    }
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, LongWritable> {
		public HashSet<String> candidates;
		private final static Text word = new Text();
	    private final static LongWritable one = new LongWritable(1);
	    
	    /**
	     * Load candidates from phase 1 into a HashSet
	     * 
	     * @param context
	     * @throws FileNotFoundException
	     * @throws IOException
	     */
		public void getCandidates(Context context) throws FileNotFoundException, IOException{
			HashSet<String> candidates = new HashSet<String>();
			
			Configuration conf = context.getConfiguration();
			
			String out = conf.get("output");
			int r = (Integer.parseInt(conf.get("r")));

			for(int i=0; i<r; i++){
				String fileName = String.format("%s/out1/part-r-0000%d", out, i);
				
				try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
				    String candidate = br.readLine();
				    candidate = br.readLine();
				    while (candidate != null) {
				    	candidates.add(candidate.split("\t")[0].trim());
				    	candidate = br.readLine();
				    }
				}
			}
			this.candidates = candidates;
		}
		
		/**
		 * Phase 2 mapper. Checks if each candidates is contained in a basket. 
		 * If it is, it sends it to the reducer.
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			getCandidates(context);
			if(value.toString().trim().length() > 1){
				List<String> basket = new ArrayList<String>();
				basket.addAll(Arrays.asList(value.toString().trim().split(",")));;
				basket.remove(0);	//Remove basket ID.
				
				for(String candidate: candidates){
					List<String> candidateList = new ArrayList<String>();
					candidateList.addAll(Arrays.asList(candidate.trim().split(",")));
					if(basket.containsAll(candidateList)){
						word.set(candidate.trim());
						context.write(word, one);
					}
				}
			}
		}
	}
	
	public static class Reducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
	
		/**
		 * Read the output from the mapper and aggregate the result.
		 * If the sum of counts from an itemset is greater the the 
		 * support threshold, then the itemset is frequent and is outputted.
		 */
	    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	    	PreProcessInput p = new PreProcessInput();
	    	int threshold = p.getThreshold();
	    	
	    	long sum = 0;
	    	for (LongWritable val : values) {
	    		sum += val.get();
	    	}
	    	if(sum >= threshold){
	    		result.set(sum);
	    		context.write(key, result);
	    	}
	    }
	}

public static void main(String[] args) throws Exception {
	long startTime = System.nanoTime();
	
	int r = 4;
	String filePartName = String.format("%s/raw_input", args[2]);
	PreProcessInput.splitFile(new File(filePartName), args[0], r);
	
	Configuration conf = new Configuration();
	Path out = new Path(args[1]);
	
	Job job1 = Job.getInstance(conf, "Phase 1");

	long startJob1 = System.nanoTime();
	job1.setMapperClass(Mapper1.class);
	job1.setCombinerClass(Reducer1.class);
	job1.setNumReduceTasks(r);
	job1.setReducerClass(Reducer1.class);
	
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(LongWritable.class);
	job1.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job1, new Path(args[0]));
	FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
	job1.waitForCompletion(true);
	
	long endJob1 = System.nanoTime();
	long durationJob1 = TimeUnit.NANOSECONDS.toMillis(endJob1-startJob1);
	System.out.println("The time it took for job 1 is " + durationJob1 + " ms");
	
	long startJob2 = System.nanoTime();
	conf.set("r", String.format("%d", r));
	conf.set("output", args[1]);
	Job job2 = Job.getInstance(conf, "Phase 2");
	job2.setMapperClass(Mapper2.class);
	job2.setCombinerClass(Reducer2.class);
	job2.setNumReduceTasks(1);
	job2.setReducerClass(Reducer2.class);
	
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(LongWritable.class);
	job2.setInputFormatClass(TextInputFormat.class);
	FileInputFormat.addInputPath(job2, new Path(args[2]));
	FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
	
	boolean succeeded2 = job2.waitForCompletion(true);
	
	long endJob2 = System.nanoTime();
	long durationJob2 = TimeUnit.NANOSECONDS.toMillis(endJob2-startJob2);
	System.out.println("The time it took for job 2 is " + durationJob2 + " ms");
	
	if(!succeeded2){
		throw new IllegalStateException("Job2 Failed");
	}
	
	long endTime = System.nanoTime();
	long duration = TimeUnit.NANOSECONDS.toMillis(endTime-startTime);
	System.out.println("The total time it took is " + duration + " ms");
}
}