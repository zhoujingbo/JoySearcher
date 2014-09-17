package org.joy.crawler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.joy.crawler.util.HTMLUUID;

/**
 *Driver for finding and tagging the duplicated outlinks
 * 
 * @author Song Liu (lamfeelin2@gmail.com)
 * 
 */
public class FilterDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new FilterDriver(), args);
	System.exit(res);
    }

    static class Dupinfo implements Writable {
	public Text URL = new Text();
	public Text source = new Text();

	public Dupinfo() {

	}

	public Dupinfo(Dupinfo another) {
	    URL = new Text(another.URL);
	    source = new Text(another.source);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	    URL.readFields(in);
	    source.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
	    URL.write(out);
	    source.write(out);
	}
    }

    /**
     * Calculate the web's UUID value
     */
    static class FileterMapper extends
	    Mapper<Text, DocumentWritale, HTMLSig, Dupinfo> {
	protected void map(
		Text key,
		DocumentWritale value,
		org.apache.hadoop.mapreduce.Mapper<Text, DocumentWritale, HTMLSig, Dupinfo>.Context context)
		throws java.io.IOException, InterruptedException {
	    // if the page has not been correctly downloaded, ignore it
	    if (!value.getDocument().trim().equals("")) {
		// UUID calculation
		long uuid = HTMLUUID.getUUID(value.getDocument());
		Logger.getLogger(this.getClass()).debug(uuid + "\t" + key);
		// fill the dup info
		Dupinfo i = new Dupinfo();
		i.URL = key;
		i.source = new Text(value.getRedirectedFrom());
		// write into output
		context.write(new HTMLSig(uuid, key.toString()), i);
	    }
	};
    }

    /**
     * Reduce by UUID.
     * 
     * @author lamfeeling
     * 
     */
    static class FilterReducer extends
	    Reducer<HTMLSig, Dupinfo, Text, OutlinksWritable> {
	protected void reduce(
		HTMLSig key,
		java.lang.Iterable<Dupinfo> values,
		org.apache.hadoop.mapreduce.Reducer<HTMLSig, Dupinfo, Text, OutlinksWritable>.Context context)
		throws java.io.IOException, InterruptedException {
	    ArrayList<Dupinfo> v = new ArrayList<Dupinfo>();

	    for (Dupinfo i : values) {
		v.add(new Dupinfo(i));
	    }
	    // choose the first one of all duplicates as the representative
	    String selectedPage = v.get(0).URL.toString();

	    for (int i = 1; i < v.size(); i++) {
		// prevent the same selected page duplicating itself
		if (!v.get(i).URL.toString().equals(selectedPage)) {
		    System.out.println(v.get(i).URL + "\tdups to\t"
			    + selectedPage);
		    // if this page has already been downloaded, remove the
		    // original outlinks
		    context.write(v.get(i).URL, new OutlinksWritable(
			    new String[0], OutlinksWritable.REMOVE_NORMAL));

		    context.write(v.get(i).source, new OutlinksWritable(
			    new String[] { selectedPage },
			    OutlinksWritable.REDIRECTED));
		}
	    }
	};
    }

    @Override
    public int run(String[] arg0) throws Exception {
	// config a job and start it
	Configuration conf = getConf();
	Job job = new Job(conf, "Filter");
	job.setJarByClass(FilterDriver.class);
	job.setMapperClass(FileterMapper.class);
	job.setReducerClass(FilterReducer.class);
	job.setSortComparatorClass(HTMLSig.SigComparator.class);
	job.setMapOutputKeyClass(HTMLSig.class);
	job.setMapOutputValueClass(Dupinfo.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(OutlinksWritable.class);
	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	String workdir = conf.get("org.joy.crawler.dir", "crawler/");
	Path path = new Path(workdir + "doc");
	FileSystem fs = FileSystem.get(conf);
	for (FileStatus s : fs.listStatus(path)) {
	    FileInputFormat.addInputPath(job, s.getPath());
	}

	Path out = new Path(workdir + "out/" + System.currentTimeMillis());
	SequenceFileOutputFormat.setOutputPath(job, out);

	return job.waitForCompletion(true) ? 0 : 1;
    }
}
