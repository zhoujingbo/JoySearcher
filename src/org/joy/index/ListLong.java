package org.joy.index;


import java.lang.Long;
import org.joy.index.Comparable;


/*
 * this basic data structure, Long is for the inverted list
 * @author: zhou jingbo
 */

public class ListLong implements Comparable<ListLong>{
	public long l;
	public ListLong(){l=0;}
	public ListLong(long ll){l=ll;}
	public long get(){return l;}
	public void set(long ll){l=ll;}
@Override
	public int compareTo(ListLong j){
	 if(l>j.get())
	    	return 1;
	    if(l==j.get())
	    	return 0;
	    else return -1;
	}
@Override
	public int comToValue(ListLong j){
	 if(l>j.get())
	    	return 1;
	    if(l==j.get())
	    	return 0;
	    else return -1;
	}
}
