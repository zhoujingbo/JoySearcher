/*
 * Downloader.java
 */
package org.joy.crawler.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Downloader do the real download action.
 * 
 * @author Song Liu (Lamfeeling@126.com)
 */
public class Downloader {
    private static String urlRegExpress = "";
    static {
	HttpURLConnection.setFollowRedirects(false);
    }
    private int timeOut = 1000 * 30;// time out for inputstream transfer and
    // socket connection
    private int maxSize = 1024 * 1024;
    private boolean cancelled;
    private boolean connecting;
    private URLConnection conn = null;
    private String redirect = null;
    private int MAX_REDIRECT = 4;
    private Thread downloadThread;
    private String charset;

    public Downloader() {
	downloadThread = Thread.currentThread();
    }

    public static void main(String[] args) throws DownloadException {
	Downloader t = new Downloader();
	Downloader.setUrlRegExpress("http://.*");
	String s1 = t.download("http://etd.library.scu.edu.tw/ETD-db/");

	System.out.println(s1);
    }

    /**
     * @param strURL
     *            the url we want to establish a connection
     * @return returns us a connected connection.
     * @throws DownloadFailedException
     *             if fails...
     */
    private void setConnectionHeader(URLConnection conn) {
	// set timeOut
	conn.setConnectTimeout(getTimeOut());
	conn.setReadTimeout(getTimeOut());
	conn.setRequestProperty("accept", "image/png,*/*;q=0.5");
	conn.setRequestProperty("connection", "Keep-Alive");
	conn.setRequestProperty("user-agent", "Baiduspider");
	conn.setRequestProperty("ua-cpu", "x86");
	conn.setRequestProperty("accept-charset", "gb2312,utf-8");
    }

    protected void openConnection(String URL) throws IOException {
	URL u = null;
	connecting = true;
	this.redirect = URL;

	// establish the connection
	u = new URL(URL);
	conn = (URLConnection) u.openConnection();
	setConnectionHeader(conn);

	// Whether we have a redicted connection
	if (HttpURLConnection.class.isInstance(conn)) {
	    HttpURLConnection c = (HttpURLConnection) conn;

	    int i = 0;
	    while (i < MAX_REDIRECT && c.getResponseCode() > 300
		    && c.getResponseCode() < 400) {
		u = new URL(u, conn.getHeaderField("Location"));
		conn = (HttpURLConnection) u.openConnection();
		setConnectionHeader(conn);
		this.redirect = u.toString();
		// if it directed to out site, throw an exception
		if (!Pattern.matches(urlRegExpress, this.redirect)) {
		    throw new ConnectException("Wrong URL pattern \t"
			    + this.redirect);
		}
		// System.err.println("从" + URL + "redirect to:" + u);
		c = (HttpURLConnection) conn;
		i++;
	    }
	    if (i == MAX_REDIRECT) {
		throw new ConnectException("Max Redirect Reached!");
	    }
	}

	// acceptable type?
	String contentType = conn.getContentType();
	if (contentType != null) {
	    contentType = contentType.toLowerCase();
	    if (!contentType.startsWith("text/html")) {
		throw new ConnectException("invalid content!");
	    }
	    // try to get charset from header
	    if (contentType.contains("charset")) {
		charset = contentType.split("charset=")[1];
	    }
	}

	connecting = false;
    }

    /**
     * download the webpage by given URL
     * 
     * @param strURL
     *            given url
     * @return the downloaded content
     * @throws if fail, throw a DownloadException
     */
    public String download(String URL) throws DownloadException {
	cancelled = false;
	BufferedReader reader = null;
	try {
	    // open connection here
	    openConnection(URL);
	    // if charset not found, try to find it in first 2048bytes in
	    // inputstream
	    byte[] buf = new byte[1024];
	    int len = 0;
	    if (charset == null) {
		InputStream in = conn.getInputStream();
		len = in.read(buf);
		int i = 0;
		while (len <= 32) {
		    if (i > 5)
			throw new DownloadException("Content too short");
		    len += in.read(buf,len,buf.length-len);
		    i++;
		}
		String header = new String(buf, 0, len);
		Pattern p = Pattern.compile(".*<meta.*content=.*charset=.*",
			Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(header);
		if (m.matches()) {
		    charset = header.toLowerCase().split("charset=")[1]
			    .replaceAll("[^a-z|1-9|\\-]", " ").trim().split(
				    "\\s+")[0];
		}
		if (charset == null || charset.trim().equals("") ) {
		    // if still not found, use utf-8 charset
		    charset = "utf-8";
		}
	    }
	    // System.out.println(charset);
	    StringBuffer pageBuffer = new StringBuffer();
	    // put our header to pageBuffer
	    pageBuffer.append(new String(buf, 0, len, charset));

	    // read the stream to String buffer
	    reader = new BufferedReader(new InputStreamReader(conn
		    .getInputStream(), charset));
	    char[] buffer = new char[2048];
	    int length = 0;
	    length = reader.read(buffer);
	    while (length != -1) {
		if (cancelled) {
		    throw new DownloadException("User Cancelled " + URL);
		}
		if (pageBuffer.length() > maxSize) {
		    throw new DownloadException("content too large " + URL);
		}
		pageBuffer.append(buffer, 0, length);
		length = reader.read(buffer);
	    }
	    return pageBuffer.toString();
	} catch (Exception e) {
	    // if fail...
	    throw new DownloadException(e);
	} finally {
	    try {
		if (reader != null) {
		    reader.close();
		}
		// if(conn!=null)
		// conn.disconnect();
	    } catch (IOException ex) {
		System.out.println("Stream Close error");
	    }
	}
    }

    public void close() {
	cancelled = true;
	if (connecting) {
	    // if(conn!=null)
	    // conn.disconnect();
	}
	try {
	    downloadThread.join();
	} catch (InterruptedException ex) {
	    ex.printStackTrace();
	}
    }

    public int getTimeOut() {
	return timeOut;
    }

    public void setTimeOut(int timeOut) {
	this.timeOut = timeOut;
    }

    public int getMaxSize() {
	return maxSize;
    }

    public void setMaxSize(int maxSize) {
	this.maxSize = maxSize;
    }

    public Thread getDownloadThread() {
	return downloadThread;
    }

    public String getRedirect() {
	return redirect;
    }

    public static void setUrlRegExpress(String urlRegExpress) {
	Downloader.urlRegExpress = urlRegExpress;
    }

    public static String getUrlRegExpress() {
	return urlRegExpress;
    }
}
