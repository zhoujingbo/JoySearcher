package org.joy.crawler;

import java.net.URL;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joy.crawler.util.Parser;

/**
 * Mapper for Parser Job.<br/>
 * This Mapper parses each document that reads from the latest doc's folder, and
 * extract their outlinks using {@link Parser} class
 * 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class ParserMapper extends
		Mapper<Text, DocumentWritale, Text, OutlinksWritable> {
	private Parser parser = new Parser();

	protected void map(Text key, DocumentWritale value, Context context)
			throws java.io.IOException, InterruptedException {
		// analyze the doc
		try {
			// use the realURL to generate the outlinks
			String realURL = key.toString();
			String text = value.getDocument();
			// set the status of hadoop and display on console
			context.setStatus(realURL);
			//System.out.println("parsing\t" + realURL);

			ArrayList<String> outlinks = new ArrayList<String>();
			for (String link : parser.extract(realURL, text, context
					.getConfiguration().get("org.joy.crawler.regEx"))) {
				if (link.contains("#")) {
					continue;
				}
				boolean invalidSuffix = false;
				String file = new URL(link).getFile();
				// filter by the suffixes
				for (String suffix : context.getConfiguration().get(
						"org.joy.crawler.suffix.disallow").split(";")) {
					if (file.toLowerCase().endsWith(suffix)) {
						invalidSuffix = true;
						break;
					}
				}
				if (!invalidSuffix)
					outlinks.add(link);
			}
			// write to output, use the real URL
			context.write(new Text(realURL), new OutlinksWritable(outlinks
					.toArray(new String[0]), OutlinksWritable.NORMAL));
			// if redirected, write another entry that points to the redirected
			// one, and make the flag "redirected" true
			if (!value.getRedirectedFrom().equals(key.toString())) {
				context.write(new Text(value.getRedirectedFrom()),
						new OutlinksWritable(new String[] { key.toString() },
								OutlinksWritable.REDIRECTED));
			}
		} catch (Exception e) {
			System.err.println(value.toString().replaceAll("\\t+", "\t"));
			e.printStackTrace();
		}
	};
}
