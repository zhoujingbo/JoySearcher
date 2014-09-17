package org.joy.crawler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver for Crawler hadoop program. <br/>
 * Crawl will download all the URL links that listed in its input folder within
 * multithreads<br/>
 * 
 * @author Song Liu (lamfeeling@126.com)
 * 
 */
public class CrawlerDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(new Configuration(), new CrawlerDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// config a job and start it
		Configuration conf = getConf();
		Job job = new Job(conf, "Download");
		job.setJarByClass(Crawler.class);
		job.setMapperClass(InverseMapper.class);
		job.setReducerClass(CrawlerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DocumentWritale.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		String workdir = conf.get("org.joy.crawler.dir", "crawler/");

		FileInputFormat.addInputPath(job, new Path(workdir + "in"));
		Path out = new Path(workdir + "doc/" + System.currentTimeMillis());
		SequenceFileOutputFormat.setOutputPath(job, out);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
