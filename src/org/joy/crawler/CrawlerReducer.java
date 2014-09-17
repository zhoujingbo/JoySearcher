package org.joy.crawler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.joy.crawler.util.Downloader;
import org.joy.crawler.util.HostShuffle;

/**
 * Reducer for Crawler hadoop program<br/>
 * The reducer in this program will split all the download tasks into sever big
 * blocks, and put them in a download queue, then download them<br/>
 * Once download started, several URLs(depending on the concurrency setting)
 * will be retrievaled from queue, and downloaded by different threads.<br/>
 * 
 * We use the semaphore to split the whole download task list, and feed them
 * into threads.
 * 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class CrawlerReducer extends
	Reducer<Text, LongWritable, Text, DocumentWritale> {

    /**
     * we need to split all url into several blocks if they are too many
     */
    private final static int BLOCK_SIZE = 1024;

    /**
     * start a muti-thread reducer to download all given urls
     */
    public void run(
	    final org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, DocumentWritale>.Context context)
	    throws IOException, InterruptedException {

	Downloader.setUrlRegExpress(context.getConfiguration().get(
		"org.joy.crawler.regEx"));

	ArrayList<String> list = new ArrayList<String>();
	// remove the ending "\t"
	while (context.nextKey()) {
	    list.add(context.getCurrentKey().toString().trim());
	}
	// Optimize the download sequence, for more info, see HostShuffle's doc
	List<String> downloadList = HostShuffle.shuffle(list);

	// put all the download operation into a threadpool
	BlockingQueue<Runnable> queue = new SynchronousQueue<Runnable>();
	// Read the concurrency setting
	int numWorker = context.getConfiguration().getInt(
		"org.joy.crawler.worker", 5);

	// semaphore used to limit the working threads
	final Semaphore s = new Semaphore(numWorker);
	// semaphore locker will separate the input urls one block at a time.
	final Semaphore sBlock = new Semaphore(BLOCK_SIZE);

	ThreadPoolExecutor threadPool = new ThreadPoolExecutor(BLOCK_SIZE,
		BLOCK_SIZE * 2, Long.MAX_VALUE, TimeUnit.SECONDS, queue);
	// fill the pool one block a time
	for (final String url : downloadList) {
	    sBlock.acquire();
	    threadPool.execute(new Runnable() {

		@Override
		public void run() {
		    try {
			// acquire the concurrency locker
			s.acquire();
			// do download operation
			Downloader t = new Downloader();
			//System.out.println("downloading\t" + url);
			// download the content of the web, and cleanup all the
			// blank
			String content = t.download(url)
				.replaceAll("\\n+", " ").replaceAll(
					"\\s+|\\t+", " ");
			// write into output and change the status
			// TODO: DO WE NEED TO SYNC VAR context?
			synchronized (context) {
			    context.setStatus(url);
			    context.write(new Text(t.getRedirect()),
				    new DocumentWritale(content, url));
			}

			// System.out.println("succeed\t" + url + "\t"
			// + content.getBytes().length / 1000 + "k");
		    } catch (Exception e) {
			synchronized (context) {
			    // if the download operation goes wrong...
			    System.err.println("ERROR:\t" + e.getMessage()
				    + "\t" + url);
			    Logger.getLogger(CrawlerReducer.class).warn(
				    url + "\t" + e);
			    try {
				// if wrong, write the empty doc
				// TODO: WE WILL ADD THE "FAIL-RETRY" FEATURE IN
				// NEXT VERSION
				context.write(new Text(url),
					new DocumentWritale());

			    } catch (Exception e1) {
				// NEVER GOES HERE
				e1.printStackTrace();
			    }
			}
		    }
		    s.release();
		    sBlock.release();
		}

	    });
	}
	// wait for completion
	threadPool.shutdown();
	threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    };
}
