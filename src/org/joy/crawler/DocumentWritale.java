package org.joy.crawler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Hadoop writable for storing web content and relative info.
 * 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class DocumentWritale implements Writable {
	private String document = new String();
	private String redirectedFrom = new String();
	private int fail = 0;

	/**
	 * construct using document content and redirect source
	 * 
	 * @param doc
	 *            document conten
	 * @param red
	 *            document conten
	 */
	public DocumentWritale(String doc, String red) {
		document = doc;
		redirectedFrom = red;
	}

	/**
	 * construct the empty document content and redirect source and increase the
	 * number of retry
	 */
	public DocumentWritale() {
		fail++;
	}

	/**
	 * get the web content
	 * 
	 * @return
	 */
	public String getDocument() {
		return document;
	}

	/**
	 * get the redirect source of current URL
	 * 
	 * @return
	 */
	public String getRedirectedFrom() {
		return redirectedFrom;
	}

	/**
	 * get the times of retry
	 * 
	 * @return
	 */
	public int getFail() {
		return fail;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		byte[] b = new byte[len];
		in.readFully(b);
		document = new String(b, "utf-8");
		redirectedFrom = in.readUTF();
		fail = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] b = document.getBytes("utf-8");
		out.writeInt(b.length);
		out.write(b);
		out.writeUTF(redirectedFrom);
		out.writeInt(fail);
	}
}
