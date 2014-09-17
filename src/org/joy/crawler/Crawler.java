package org.joy.crawler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Crawler is a shell wraps all hadoop jobs of Joycrawler As a standard hadoop
 * program, it launches "Crawl Sequence" when running.
 * 
 * @author Song Liu (lamfeeling@126.com)
 * 
 */
public class Crawler extends Configured implements Tool {

    public static Logger logger = Logger.getLogger(Crawler.class);

    public static void main(String[] args) throws Exception {
    	String aa[]={"seeds.txt","1"};
    	args=aa;
	PropertyConfigurator.configure("conf/log4j.properties");
	logger.info(System.getProperties());
	int res = ToolRunner.run(new Configuration(), new Crawler(), args);
	System.exit(res);
    }
public  int setRun(String args[]) throws Exception
    {
	
    	PropertyConfigurator.configure("conf/log4j.properties");
    	logger.info(System.getProperties());
    	int res = ToolRunner.run(new Configuration(), new Crawler(), args);
    	return res;
    	//System.exit(res);
    }
    /**
     * Launch the crawl sequence using the given arguments
     */
    @Override
    public int run(String[] args) throws Exception {
	// add resources to hadoop configuration
	getConf().addResource("joycrawler-site.xml");
	getConf().set("org.joy.crawler.seeds", args[0]);

	// make sure our working directory is empty
	String workdir = getConf().get("org.joy.crawler.dir");
	FileSystem fs = FileSystem.get(getConf());
	if (fs.exists(new Path(workdir))) {
	    fs.delete(new Path(workdir), true);
	}
	fs.mkdirs(new Path(workdir));

	// inject the initial seeds
	fs.copyFromLocalFile(new Path(getConf().get("org.joy.crawler.seeds")),
		new Path(getConf().get("org.joy.crawler.dir") + "in/init.txt"));

	int numTurn = Integer.parseInt(args[1]);
	// luanch the crawl sequence, we will crawl numTurn turns
	for (int i = 0; i < numTurn; i++) {
	    // if there are no input URLs, we can finish our work
	    boolean finished = true;
	    FileStatus[] status = fs.listStatus(new Path(getConf().get(
		    "org.joy.crawler.dir")
		    + "in/"));
	    for (FileStatus s : status) {
		if (!s.getPath().getName().endsWith(".crc") && s.getLen() != 0) {
		    finished = false;
		    break;
		}
	    }
	    if (finished) {
		System.out.println("Crawling finished!");
		break;
	    }
	    System.out.println("downloading...");
	    int res = ToolRunner.run(getConf(), new CrawlerDriver(), args);
	    if (res != 0) {
		return 1;
	    }
	    System.out.println("parsing...");
	    res = ToolRunner.run(getConf(), new ParserDriver(), args);
	    if (res != 0) {
		return 1;
	    }
	    System.out.println("optmizing...");
	    res = ToolRunner.run(getConf(), new OptimizerDriver(), args);
	    if (res != 0) {
		return 1;
	    }
	    System.out.println("The "+(i+1)+"th level crawling finished\n");
	}
	System.out.println("\nfiltering...");
	int res = ToolRunner.run(getConf(), new FilterDriver(), args);
	if (res != 0) {
	    return res;
	}
	System.out.println("\nmerging...");
	return ToolRunner.run(getConf(), new MergeDriver(), args);
    }
}
