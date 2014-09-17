package org.joy.index;


import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.cyberneko.html.parsers.DOMParser;
import org.joy.crawler.DocumentWritale;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;


public class DataCleanerDriver  extends Configured implements Tool{
	
	
	 /**
     * This mapper will extract all the text content from HTML, and simply write
     * it to output.
     * 
     * 
     */
	 static class DataCleanerMapper extends
	    Mapper<Text, DocumentWritale, Text, Text> {
	protected void map(
				Text key,
				DocumentWritale value,
				org.apache.hadoop.mapreduce.Mapper<Text, DocumentWritale, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
	    	try {
	    		context.setStatus(key.toString());
	    		System.out.println("analyzing:" + key);
	    		// Simply try to analyze the whole document.
	    		// HTMLDocument doc = HTMLDocument
	    		// .createHTMLDocument(key.toString(), value.getDocument()
	    		// .replaceAll("\\n", " "));
	    		// String content = doc.getContent().replaceAll("\\r*\\n+", " ")
	    		// .trim();
	    		DOMParser p = new DOMParser();
	    		p.parse(new InputSource(new StringReader(value.getDocument())));
	    		Document doc = p.getDocument();
	    		String content = doc.getElementsByTagName("BODY").item(0)
	    		.getTextContent().replaceAll("\\n+", " ").trim();
	    		context.write(key, new Text(content));//the url also will be part of the web content
	    	} catch (Exception e) {
	    		// TODO Auto-generated catch block
	    		e.printStackTrace();
	    	}
		};
	
	
	
 }
	 
	 public static class DataCleanerReducer extends 
	 					Reducer<Text,Text,Text,Text> {
		 Text result=new Text("");
	 public void reduce(Text key, Iterable<Text> values, 
				 org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Text>.Context context
		 		) throws IOException, InterruptedException {
			 	String sum ="";
			 	for (Text val : values) {
			 		sum += val.toString();
			 	}
			 	result.set(sum);
			 	context.write(key, result);
		 }
}

	

	public static void main(String[] args)throws Exception {
					int res = ToolRunner.run(new Configuration(), new DataCleanerDriver(), args);
					System.out.print("the cleaner has finished its work! the tag is removed!\n");
					System.exit(res);
	}
	public int setRun()throws Exception
	{
				String aa[]={"",""};
				int res = ToolRunner.run(new Configuration(), new DataCleanerDriver(),aa);
				System.out.print("the cleaner has finished its work! the tag is removed!\n");
				return res;
			//	System.exit(res);
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	 public int run(String[] arg0) throws Exception {
			// config a job and start it
			Configuration conf = getConf();
			Job job = new Job(conf, "DataCleaner");
			job.setJarByClass(DataCleanerDriver.class);
			job.setMapperClass(DataCleanerMapper.class);
			job.setReducerClass(DataCleanerReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);

			FileSystem fs = FileSystem.get(conf);
			String workdir = conf.get("org.joy.crawler.dir", "crawler/");

			for (FileStatus stat : fs.listStatus(new Path(workdir + "doc"))) {
			    FileInputFormat.addInputPath(job, stat.getPath());
			}

			Path out = new Path(workdir + "content/");
			fs.delete(out, true);
			FileOutputFormat.setOutputPath(job, out);
            System.out.print("the cleaner start cleaning data, please wait...\n");
			return job.waitForCompletion(true) ? 0 : 1;
		    }

}
