package org.joy.index;

import java.io.IOException;
import java.util.* ;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;




public class IndexConstructorDriver extends Configured implements Tool{
	 
	private final static String tokenDelimiter=".?!,:;-_()\"\"'[]{}&|<>\\/ ¡°¨C@¡®\\t\\n\\v#$%^&+~=";  // the delimiter for StringTokenizer, 
    
	private final static String stopWords[] = //the stop list
	    	          {"a", "as","about","am", "an", "and", "are", "as", "at", "be", "but", "by",
	    	  
	    	           "for", "has", "have", "he", "his", "her", "in", "is", "it", "its",
	    	  
	    	          "of", "on", "or", "she", "that", "the", "their", "they", "this",
	    	 
	    	          "to", "was", "which", "who", "will", "with", "you"};

    
    public final static Map<String, String> hmStopWord= new HashMap<String, String> ();
 
/*
 * building the index
 * @author£º Zhou Jingbo
 * 
 */
  public static class IndexConstructorMapper 
  	extends Mapper<Text, Text, Text, Text> {
    private Text location;// the location, or the document number 
    private String keyWord="";
    String cache[];
    public void map(
    		Text key, 
    		Text val,
    		org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Text>.Context context
    		) throws IOException, InterruptedException{
    		int i=0,n=0,j=0,lj=0,hj=0;
    		String tem="";
    		
    		initStopWordsMap();// initialize  the stop list
    		String line = val.toString();
    		StringTokenizer itr = new StringTokenizer(line.toLowerCase(),tokenDelimiter);//set delimiter
    		n=itr.countTokens();
    		cache=new String[n];
    		for(i=0;i<n;i++){
    			cache[i]=new String("");//initialize the cache
    		}
    		i=0;
    	while(itr.hasMoreTokens()){
    			cache[i]=itr.nextToken();//padding the cache with the words of the content
    			i++;
    		}
    		for(i=0;i<n;i++){
    			keyWord=cache[i];
    			keyWord=keyWord.trim();
    			if(!hmStopWord.containsKey(keyWord))
    			{ 
    			  lj=i-10;hj=i+10;
    			  if(lj<0) lj=0;
    			  if(hj>n) hj=n;
    			  tem=" ";
    			  for(j=lj;j<hj;j++)
    				  tem+=cache[j]+" ";
    			  location=new Text();
    			  location.set(key.toString()+tem);
    			context.write(new Text(keyWord), location);
    			}
    		}
    	}
  	}

  
  
//////////////////////////////////////////////////////////////////

  public static class IndexConstructorReducer extends
		Reducer<Text, Text, Text, InvertedListWritable>{
	 // private IntWritable result = new IntWritable();
	  Text list=new Text("");
	  ListURL dr;
	  String url="",abs="";
    public void reduce(
    		Text key, 
    		Iterable<Text> values,
    		org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, InvertedListWritable>.Context context
    		)throws IOException, InterruptedException {
    		InvertedListWritable invertedList=new InvertedListWritable();
    		for(Text k : values){
    			StringTokenizer itr=new StringTokenizer(k.toString());
    			url="";abs="";
    			if(itr.hasMoreTokens()) url=itr.nextToken();
    			while(itr.hasMoreTokens()){
    			abs+=itr.nextToken()+" ";
    			}
    			dr=new ListURL(url,abs);
    			invertedList.paddingValueKey(dr);
    }
    		invertedList.quickSortNodeKey();
    		context.write(key, invertedList);
    }
  }
  
  
  
/////////////////////////////////////////////////////////////////////////////////
  /**
   * The actual main() method for our program; this is the
   * "driver" for the MapReduce job.
   */
  public static void main(String[] args) throws Exception{
	 int res = ToolRunner.run(new Configuration(), new IndexConstructorDriver(), args);
      System.exit(res);
  }
  
  
  /*
   * this can execute the function of main;
   * @author : Zhou Jingbo
   */
  public int setRun()throws Exception{
	  String args[]=new String[0];
	  int res = ToolRunner.run(new Configuration(), new IndexConstructorDriver(), args);
		System.out.print("the indxer has finished its work! \nthe index has been constructed, please enjoy JoySearcher!\n");
	  return res;
     // System.exit(res);
  }
  
  
  
  @Override
  public int run(String[] arg0) throws Exception {
		// config a job and start it
		Configuration conf = getConf();
		Job job = new Job(conf, "Index construction..");
		job.setJarByClass(IndexConstructorDriver.class);
		job.setMapperClass(IndexConstructorMapper.class);
	    job.setReducerClass(IndexConstructorReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InvertedListWritable.class);
	   job.setMapOutputKeyClass(Text.class); 
	  job.setMapOutputValueClass(Text.class); 
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// can add the dir by the config
		FileSystem fs = FileSystem.get(conf);
		String workdir = conf.get("org.joy.crawler.dir", "crawler/");
		fs.delete(new Path(workdir+"indexOutput/"),true);
		 FileInputFormat.addInputPath(job, new Path(workdir+"content/"));
		FileOutputFormat.setOutputPath(job, new Path(workdir+"indexOutput/"));
		 System.out.println("indexer starts to work, it begins to construct the index, please wait ...\n");
		return job.waitForCompletion(true) ? 0 : 1;
	    }
  
  
  
  
  /*
   * Init of stop words hash map
   */
public  static void initStopWordsMap() {
	  
      for (int i = 0; i < stopWords.length; i++) 

          hmStopWord.put(stopWords[i], null);

  }



public static void WriteToTemp(String temp,Configuration conf,String dir) throws Exception
{
	 FileSystem fs = FileSystem.get(conf);
	 java.io.OutputStream out = fs.create(new Path(dir));
	 out.write(temp.getBytes());
	 out.flush();
	 out.close();
}
}

  
  
  



//junk code; code warehouse
/*
StringTokenizer itr = new StringTokenizer(val.toString());
while (itr.hasMoreTokens()) {
  word.set(itr.nextToken());
  context.write(word, one);
}*/



//  private final static IntWritable one = new IntWritable(1);



/*word1=new Text("bad");
//context.write(key,word1 );
  int sum = 0;
  for (IntWritable val : values) {
    sum += val.get();
  }
  result.set(sum);
  context.write(key, result);*/
