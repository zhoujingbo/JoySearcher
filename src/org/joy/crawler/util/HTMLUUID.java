package org.joy.crawler.util;

import java.util.UUID;

public class HTMLUUID {
    public static long getUUID(String str) {
	return UUID.nameUUIDFromBytes(str.replaceAll("[0-9]+", "").trim().getBytes()).getLeastSignificantBits();
    }
}
