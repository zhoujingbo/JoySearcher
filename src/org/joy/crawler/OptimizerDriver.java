package org.joy.crawler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Optimizer Driver for Optimizer job<br/>
 * Optimizer is a job to find the uncrawled URLs in last turn's outlinks, and
 * output them to "in" folder.
 * 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class OptimizerDriver extends Configured implements Tool {

	static class OptimizerReducer extends
			Reducer<Text, BooleanWritable, Text, Text> {
		protected void reduce(
				Text key,
				java.lang.Iterable<BooleanWritable> values,
				org.apache.hadoop.mapreduce.Reducer<Text, BooleanWritable, Text, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			boolean visited = false;
			for (BooleanWritable b : values) {
				if (b.get()) {
					visited = true;
				}
			}
			if (!visited) {
				context.write(new Text(key), new Text(""));
			} else {
				// System.out.println(key);
			}
		};
	}

	/**
	 * Read all the links from outlinks folder, and mark them with true
	 * if it is the origin link and false if outlink.
	 * 
	 * @author Administrator
	 * 
	 */
	static class OptimizerMapper extends
			Mapper<Text, OutlinksWritable, Text, BooleanWritable> {

		protected void map(
				Text key,
				OutlinksWritable outLinks,
				org.apache.hadoop.mapreduce.Mapper<Text, OutlinksWritable, Text, BooleanWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			context.write(key, new BooleanWritable(true));
			for (String link : outLinks.getOutlinks())
				context.write(new Text(link), new BooleanWritable(false));
		};
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OptimizerDriver(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "Optimize");
		job.setJarByClass(Crawler.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(OptimizerMapper.class);
		job.setReducerClass(OptimizerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BooleanWritable.class);

		String workdir = conf.get("org.joy.crawler.dir");
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(workdir + "in");
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Path inPath = new Path(workdir + "out");

		for (FileStatus s : fs.listStatus(inPath)) {
			Path sub = s.getPath();
			FileInputFormat.addInputPath(job, sub);
		}

		FileOutputFormat.setOutputPath(job, new Path(workdir + "in"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
