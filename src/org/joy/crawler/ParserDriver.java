package org.joy.crawler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Parser Drvier drives the a parsing job.<br/>
 * This job will analyze all the pages that crawler job downloads. 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class ParserDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("joycrawler-site.xml");
		int res = ToolRunner.run(conf, new ParserDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "Analyze");
		job.setJarByClass(Crawler.class);
		job.setMapperClass(ParserMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OutlinksWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		String workdir = conf.get("org.joy.crawler.dir", "crawler/");
		FileSystem fs = FileSystem.get(conf);
		// find the latest doc folder that downloads
		long latest = 0;
		Path inPath = null;
		for (FileStatus stat : fs.listStatus(new Path(workdir + "doc"))) {
			if (stat.getModificationTime() > latest) {
				inPath = stat.getPath();
			}
		}
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, new Path(workdir + "out/"
				+ System.currentTimeMillis()));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
