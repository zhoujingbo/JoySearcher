package org.joy.crawler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Driver for Merge Program. <br/>
 * Merge Program that runs two independent jobs that merges doc and outlink
 * folders in work directory, this also remove the duplicate webpages and
 * outlinks in those folders<br/>
 * 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class MergeDriver extends Configured implements Tool {

    /**
     * Reducer for Doc folder
     * 
     * @author Administrator
     * 
     */
    static class ReducerDoc extends
	    Reducer<Text, DocumentWritale, Text, DocumentWritale> {
	protected void reduce(
		Text key,
		java.lang.Iterable<DocumentWritale> values,
		org.apache.hadoop.mapreduce.Reducer<Text, DocumentWritale, Text, DocumentWritale>.Context context)
		throws java.io.IOException, InterruptedException {
	    for (DocumentWritale w : values) {
		context.write(key, w);
		return;
	    }
	};
    }

    /**
     * Reducer for outlinks folder
     * 
     * @author Administrator
     * 
     */
    static class ReducerLink extends
	    Reducer<Text, OutlinksWritable, Text, OutlinksWritable> {
	protected void reduce(
		Text key,
		java.lang.Iterable<OutlinksWritable> values,
		org.apache.hadoop.mapreduce.Reducer<Text, OutlinksWritable, Text, OutlinksWritable>.Context context)
		throws java.io.IOException, InterruptedException {
	    // sort the values
	    List<OutlinksWritable> v = new ArrayList<OutlinksWritable>();
	    for (OutlinksWritable w : values) {
		v.add(new OutlinksWritable(w));
	    }
	    Collections.sort(v);
	    
	    boolean removeNormal = false;
	    OutlinksWritable normal = null, redirect = null;

	    //find the latest normal and redirected outlinks, and if remove_normal flag is tre
	    for (OutlinksWritable w : v) {
		if (w.getTypeOfOutlink() == OutlinksWritable.REMOVE_NORMAL) {
		    removeNormal = true;
		}
		if (w.getTypeOfOutlink() == OutlinksWritable.NORMAL) {
		    normal = w;
		}
		if (w.getTypeOfOutlink() == OutlinksWritable.REDIRECTED) {
		    redirect = w;
		}
	    }

	    //write the normal outlinks if not removed
	    if (!removeNormal && normal != null) {
		context.write(key, normal);
	    }
	    //write the redirected outlinks
	    if (redirect != null) {
		context.write(key, redirect);
	    }
	};
    }

    @Override
    public int run(String[] arg0) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf, "merge");
	job.setJarByClass(Crawler.class);
	job.setReducerClass(ReducerDoc.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DocumentWritale.class);
	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	String workdir = conf.get("org.joy.crawler.dir", "crawler/");
	FileSystem fs = FileSystem.get(conf);

	// add all the doc folders
	for (FileStatus stat : fs.listStatus(new Path(workdir + "doc"))) {
	    FileInputFormat.addInputPath(job, stat.getPath());
	}

	SequenceFileOutputFormat.setOutputPath(job, new Path(workdir
		+ "doc_tmp/" + System.currentTimeMillis()));
	// merge doc folders
	int res = job.waitForCompletion(true) ? 0 : 1;

	if (res == 0) {
	    // replace the previous folder with newly merged folder
	    fs.delete(new Path(workdir + "doc"), true);
	    fs.rename(new Path(workdir + "doc_tmp"), new Path(workdir + "doc"));
	    System.out.println("MergeDoc Done!");

	    // start another job that do the same thing for outlinks folder
	    job = new Job(conf, "merge");
	    job.setJarByClass(Crawler.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(OutlinksWritable.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setReducerClass(ReducerLink.class);
	    for (FileStatus stat : fs.listStatus(new Path(workdir + "out/"))) {
		FileInputFormat.addInputPath(job, stat.getPath());
	    }
	    SequenceFileOutputFormat.setOutputPath(job, new Path(workdir
		    + "out_tmp/" + System.currentTimeMillis()));
	    res = job.waitForCompletion(true) ? 0 : 1;
	    if (res == 0) {
		fs.delete(new Path(workdir + "out/"), true);
		fs.rename(new Path(workdir + "out_tmp/"), new Path(workdir
			+ "out/"));
		System.out.println("MergeLinks Done!");
		return 0;
	    }
	}
	return 1;
    }

}
