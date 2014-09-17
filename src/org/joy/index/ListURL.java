package org.joy.index;




/*
 * this data structure is for the inverted list, 
 * it represnts the unique id of the document or the web page
 * @author: zhou jingbo
 */
public class ListURL implements Comparable<ListURL>{

	private String url;
	private String abs;
	public ListURL(String ls){ url=ls;abs="";}
	public ListURL(String u,String con){url=u;abs=con;}
	public ListURL(){url="";abs="";}
	public String getURL(){return url;}
	public String getabs(){return abs;}
	public void setURL(String doc){url=doc;}
	public void setAbstract(String a){abs=a;}
	public void set(String ss,String a){url =ss;a=abs;}
	
	public boolean equals(ListURL j)
	{	if(url.equals(j.getURL()))
		return true;
	return false;
	}
@Override
	public int compareTo(ListURL c){
		return url.compareTo(c.getURL());
	}
@Override
	public int comToValue(ListURL c){
	     return url.compareTo(c.getURL());
}
	
	
	
}





