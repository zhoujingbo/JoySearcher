package org.joy.crawler.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Stack;

/**
 * Host shuffle makes the urls that share the same host name will have the
 * largest distance in the crawl queue.
 * 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class HostShuffle {
	static class Item implements Comparable<Item> {
		int age;
		String host;

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return host + "	" + age;
		}

		@Override
		public int compareTo(Item o) {
			// TODO Auto-generated method stub
			return age - o.age;
		}

	}

	public static List<String> shuffle(List<String> origin) {
		Hashtable<String, Stack<String>> table = new Hashtable<String, Stack<String>>();
		for (String u : origin) {
			URL url;
			try {
				url = new URL(u);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}
			if (table.get(url.getHost()) == null) {
				table.put(url.getHost(), new Stack<String>());
			}
			table.get(url.getHost()).add(u);
		}

		ArrayList<Item> hosts = new ArrayList<Item>();
		for (String h : table.keySet()) {
			Item i = new Item();
			i.age = 0;
			i.host = h;
			hosts.add(i);
		}
		Collections.sort(hosts);

		ArrayList<String> res = new ArrayList<String>();
		int age = 1;
		while (res.size() != origin.size()) {

			String url = table.get(hosts.get(0).host).pop();
			res.add(url);
			hosts.get(0).age = age;

			if (table.get(hosts.get(0).host).size() == 0) {
				table.remove(hosts.get(0).host);
				hosts.remove(0);
			}
			Collections.sort(hosts);
			age++;
		}
		return res;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(shuffle(Arrays.asList(new String[] {
				"http://google.com/1", "http://abc.com/1",
				"http://google.com/2", "http://def.com/2", "http://def.com/1",
				"http://abc.com/2" })));
	}

}
