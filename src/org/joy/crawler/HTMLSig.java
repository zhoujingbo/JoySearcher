package org.joy.crawler;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class HTMLSig extends Text {

	static class SigComparator extends Text.Comparator {

		static {
			WritableComparator.define(HTMLSig.class, new SigComparator());
		}

		private static byte[] copy(byte[] src, int s, int l) {
			byte[] dest = new byte[l];
			for (int i = 0; i < l; i++) {
				dest[i] = src[s + i];
			}
			return dest;
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			HTMLSig hs1 = new HTMLSig(copy(b1, s1 + 1, l1 - 1));
			HTMLSig hs2 = new HTMLSig(copy(b2, s2 + 1, l2 - 1));

			if (hs1.getUUID() == hs2.getUUID()) {
				Logger.getLogger(this.getClass()).debug(
						hs1.getURL() + "\t hash dups to \t" + hs2.getURL());
				return 0;
			}
			String url1 = hs1.getURL(), url2 = hs2.getURL();
			String longer = url1.length() > url2.length() ? url1 : url2;
			String shorter = url1.length() < url2.length() ? url1 : url2;

			if (longer.startsWith(shorter)) {
				String suffix = longer.substring(shorter.length());
				if (suffix.toLowerCase().matches(
						"(default\\.aspx|index\\.asp|index\\.php|index\\.htm|"
								+ "index\\.html|index\\.py|index\\.jsp)")) {
					Logger.getLogger(this.getClass()).debug(
							longer + "\t URL dups to \t" + shorter);
					return 0;
				}
			}
			return compareBytes(b1, s1, l1, b2, s2, l2);

		}

		public static void main(String[] args) throws Exception {
			String u1 = "http://dfa.com/dfa%d?sf/";
			String u2 = "http://dfa.com/dfa%d?sf/iNdex.htML";
			int a = new SigComparator().compare(u1.getBytes("utf-8"), -1, u1
					.getBytes("utf-8").length, u2.getBytes("utf-8"), -1, u2
					.getBytes("utf-8").length);
			System.out.println(a);
		}

		@Override
		public int compare(Object arg0, Object arg1) {
			// TODO Auto-generated method stub
			return 0;
		}
	}

	public HTMLSig(long uuid, String URL) {
		super("<sep>\t" + uuid + "\t" + URL + "\t</sep>");
	}

	public HTMLSig() {
		// TODO Auto-generated constructor stub
	}

	public HTMLSig(byte[] b) {
		super(b);
	}

	public long getUUID() {
		return Long.parseLong(this.toString().split("\t")[1]);
	}

	public String getURL() {
		return this.toString().split("\t")[2];
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HTMLSig s = new HTMLSig(123, "dd");
		System.out.println(s.getURL());
		System.out.println(s.getUUID());
	}

}
