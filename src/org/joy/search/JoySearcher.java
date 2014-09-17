package org.joy.search;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;


import org.joy.index.*;




public class JoySearcher extends Configured implements Tool{

	  public static class SearcherMapper 
	  	extends Mapper<Text, InvertedListWritable, Text, InvertedListWritable>{
		  private  Pattern pattern;
		   String keyStr;
		   int keyNum;
		   String fileKeyStr;
		   String recogPattern;
		   Matcher matcher;
		  protected void map(
					Text key,
					InvertedListWritable value,
					org.apache.hadoop.mapreduce.Mapper<Text, InvertedListWritable, Text, InvertedListWritable>.Context context)
					throws java.io.IOException, InterruptedException {
			  	
			  		Text searchResultText=new Text("");
			  		Text keyOut=new Text();
				    try {
				    recogPattern=context.getConfiguration().get("mapred.mapper.pattern");	
				    keyStr=context.getConfiguration().get("mapred.mapper.keywords");
				    
				    keyOut.set("	the recognize pattern is '"+recogPattern+"';\n	the key wrods are \""+keyStr+ "\" \n	the results are as following:\n");
				    StringTokenizer itr = new StringTokenizer(keyStr);//set delimiter
				    keyNum=itr.countTokens();
				    String keyStrToken[]=new String[keyNum];

				   // System.exit(9);
				    for(int i=0;i<keyNum&&itr.hasMoreElements();i++)
				    	{keyStrToken[i]=itr.nextToken();
				    	}
				    fileKeyStr= key.toString();
				   for(int i=0;i<keyNum;i++){
					   if(recogPattern.equals("-regex")){
				    	pattern = Pattern.compile(keyStrToken[i]);
				    	matcher= pattern.matcher(fileKeyStr);
				    	if(matcher.find()){
				   	    searchResultText.set(value.toString());
						   context.write(keyOut, value);
				    	}
				    	}
					   else{
						   if(fileKeyStr.equals(keyStrToken[i])){
						   	   searchResultText.set(value.toString());
								   context.write(keyOut, value);
						    	}
						   
					   }

					   }
				   }catch (Exception e){e.printStackTrace();}
	  }
}
	  

	  public static class SearcherReducer extends

		Reducer<Text, InvertedListWritable, Text, Text>{

			Text listKey=new Text("");
			Text listValue=new Text("");
			
			public void reduce(
						Text key, 
						java.lang.Iterable<InvertedListWritable> values,
						org.apache.hadoop.mapreduce.Reducer<Text, InvertedListWritable, Text, Text>.Context context
						)throws IOException, InterruptedException {
						int listNum=0;
						//Iterable<InvertedListWritable> count=values;
						Iterator<InvertedListWritable> ac=values.iterator();
						ArrayList<InvertedListWritable> iListArray=new ArrayList<InvertedListWritable>();
						InvertedListWritable mergeList=new InvertedListWritable();
						
						
						InvertedListWritable temp;
						while(ac.hasNext()){
						temp=(InvertedListWritable)ac.next().clone();
							iListArray.add(temp);
						}
						
						listNum=iListArray.size();
						InvertedListWritable iList[]=new InvertedListWritable[listNum];
					//	iListArray.toArray(iList);
						for(int i=0;i<listNum;i++){
							iList[i]=new InvertedListWritable();
						}
						for(int i=0;i<listNum;i++){
							//temp=new InvertedListWritable();
							//temp=iListArray.get(i);
							//iList[i]=temp;
							iList[i]=(InvertedListWritable)iListArray.get(i).clone();
							//iList[i].quickSortNodeKey();
						}
						mergeList.mergeLapList(iList,listNum);
						mergeList.quickSortNodeValue();
						listValue.set(mergeList.toString());
						//listValue.set("good"+listNum);
						//key.set("first"+iListArray.get(0).getKey(2)+"  second "+iListArray.get(1).getKey(2)+"  sum"+mergeList.getLength()+"array"+iListArray.size()+"  num0"+num[0]+"  num1"+num[1]);
						context.write(key, listValue);
	    }
	  }

	
	public static void main(String[] args) throws Exception{
				int res = ToolRunner.run(new Configuration(), new JoySearcher(), args);
				System.exit(res);
	}
	
	public int setRun(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new JoySearcher(), args);
		return res;
}
	

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
@Override
	 public int run(String[] args) throws Exception {
			// config a job and start it
			Configuration conf = getConf();
			int i=0;
			 if (args.length < 2 || (args.length==1&&(args[1]=="-regex"||args[1]=="-full")) ) {
			      System.out.println("the keywords is necessery, please give a keyword~~\n " +
			      		"the format is \n [-regex/-full] [keywrods]");
			      
			      ToolRunner.printGenericCommandUsage(System.out);
			      return -1;
			    }
			 //set the pattern, it can be 
			if(args[1].equals("-regex")){
				conf.set("mapred.mapper.pattern","-regex");
				i=2;
			}
			else if(args[1].equals("-full")){
				conf.set("mapred.mapper.pattern", "-full");
				i=2;
			}
			else {conf.set("mapred.mapper.pattern", "-full"); i=1;}
			
			String aa="";
			 for(;i<args.length;i++){
					aa+=args[i]+" ";
			}
			conf.set("mapred.mapper.keywords",aa);
			  
			//JobConf job = new Job(conf, "Searcher");
			
			Job job=new Job(conf,"Searcher");
		//	job.g
			job.setJarByClass(JoySearcher.class);
			job.setMapperClass(SearcherMapper.class);
			job.setReducerClass(SearcherReducer.class);
			//job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(InvertedListWritable.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			FileSystem fs=FileSystem.get(conf);
			String workdir=conf.get("org.joy.crawler.dir", "crawler/");
		//	for (FileStatus stat : fs.listStatus(new Path(workdir + "indexOutput"))) {
			 //   FileInputFormat.addInputPath(job, stat.getPath());
		//	}
			FileInputFormat.addInputPath(job, new Path(workdir+"indexOutput/"));
			// can add the dir by the config
            Path out =new Path(workdir + "searchResult");
            fs.delete(out,true);
            FileOutputFormat.setOutputPath(job,out);
            System.out.print("JoySearcher start searching the keywrods!\n");
            int jobRes=0;
            jobRes=job.waitForCompletion(true) ? 0 : 1;
            System.out.print("JoySearcher has finished its work!\n");
           // ReadFile(conf,new Path(workdir + "searchResult/result"));//not safe
			return jobRes;
		//    }
	 }
	
public static void ReadFile(Configuration conf,String dir) throws Exception
{
	 FileSystem fs = FileSystem.get(conf);
	 java.io.InputStream in = fs.open(new Path(dir));
	 int len=in.available();
	 byte b[]=new byte[len];
	 in.read(b);
	 System.out.print(b);
	  in.close();
}

	 public static void WriteToTemp(String temp,Configuration conf,String dir) throws Exception
	 {
		 FileSystem fs = FileSystem.get(conf);
		 java.io.OutputStream out = fs.create(new Path(dir));
		 java.io.InputStream in = fs.open(new Path(dir));
		 out.write(temp.getBytes());
		 out.flush();
		 out.close();
	 }

}
