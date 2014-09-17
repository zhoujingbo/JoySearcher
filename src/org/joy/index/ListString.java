package org.joy.index;
import java.lang.String;

import org.joy.index.Comparable;

/*
 * this is the basic types for the String in the inverted list
 * @zhou jingbo
 */
public class ListString implements Comparable<ListString>{
	private String string;
	public ListString(String ls){ string=ls;}
	public ListString(){string="";}
	public String get(){return string;}
	public void set(String ss){string =ss;}
	
	public boolean equals(ListString j)
	{	if(string.equals(j.get()))
		return true;
	return false;
	}
@Override
	public int compareTo(ListString c){
		return string.compareTo(c.get());
	}
@Override
	public int comToValue(ListString c){
	     return string.compareTo(c.get());
}
}



