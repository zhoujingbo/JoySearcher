package org.joy.crawler.util;

/**
 * Download Exception that thrown by Downloader Class
 * 
 * @author Song Liu (Lamfeeling@126.com)
 */
public class DownloadException extends Exception {

	public DownloadException(Throwable ex) {
		super(ex);
	}

	public DownloadException() {
	}

	public DownloadException(String msg) {
		super(msg);
	}

}
