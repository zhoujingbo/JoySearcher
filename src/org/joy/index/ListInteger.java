package org.joy.index;

import java.lang.Integer;
import org.joy.index.Comparable;


/*
 * this basic data structure, Int, is for the inverted list
 * @author: zhou jingbo
 */
public class ListInteger implements Comparable<ListInteger> {
	public int integer;
	public ListInteger(int j){ integer=j;}
	public ListInteger(){integer=0;}
	public int get(){return integer;}
	public void set(int i){integer=i;}
	public boolean equals(ListInteger j)
	{	if(integer==j.get())
		return true;
	return false;
		
	}
@Override
	public int compareTo(ListInteger j){
	    if(integer>j.integer)
	    	return 1;
	    if(integer==j.integer)
	    	return 0;
	    else return -1;
	}
@Override
	public int comToValue(ListInteger j){
	 if(integer>j.integer)
	    	return 1;
	    if(integer==j.integer)
	    	return 0;
	    else return -1;
}

}
