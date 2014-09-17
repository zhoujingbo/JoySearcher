package org.joy.index;
import org.joy.index.*;
import java.util.*;
public class IndexNode<NODEKEY extends Comparable<? super NODEKEY>,NODEVALUE extends Comparable<? super NODEVALUE>>  implements Comparable<IndexNode<NODEKEY,NODEVALUE>>{
	private NODEKEY key;
	private NODEVALUE value;
	public IndexNode()
	{
	key=null;
	value=null;
	}
	public IndexNode(NODEKEY k, NODEVALUE v)
	{
		key=k;
		value=v;
	}
	/*
	 * get key
	 */
	public NODEKEY getKey()
	{
		return key;
	}
	/*
	 * get value
	 */
	public NODEVALUE getValue()
	{
		return value;
	}

	public void setValue(NODEVALUE D)
	{
		this.value=D;
	}
	public void setKey(NODEKEY k){
		this.key=k;
	}
	/*
	 * compare the value of list node
	 */
@Override
public int compareTo(IndexNode<NODEKEY,NODEVALUE> o){
	  int KeyCompareTo = this.getKey().compareTo(o.getKey());  
	           
	         if (KeyCompareTo > 0)  
	             return 1;  
	         else if (KeyCompareTo < 0)  
	             return -1;  
	         else  
	            return 0;  
	}
/*
 * test whether the value are equle of two node
 */
@SuppressWarnings("unchecked")  
     public boolean equals(Object o){  
         if(o instanceof IndexNode)  
             if(this.getKey().compareTo(((IndexNode<NODEKEY,NODEVALUE>)o).getKey())==0)  
                return true;  
         return false;  
     }
@Override 
public int comToValue(IndexNode<NODEKEY,NODEVALUE> o){
	int vct =this.getValue().comToValue(o.getValue());
	
	if(vct>0)
		return 1;
	else if (vct<0)
		return -1;
	else return 0;
	
}

}



































































/*
@Override
public int compareToKey(IndexNode<NODEKEY,NODEVALUE> o){
	  int keyCompareTo = this.getKey().compareToKey(o.getKey());  
	           
	         if (keyCompareTo > 0)  
	             return 1;  
	         else if (keyCompareTo < 0)  
	             return -1;  
	         else  
	            return 0;  
	}
	*/
/*
@SuppressWarnings("unchecked")  
     public boolean equalsKey(Object o){  
         if(o instanceof IndexNode)  
             if(this.getKey().compareToKey(((IndexNode<NODEKEY,NODEVALUE>)o).getKey())==0)  
                return true;  
         return false;  
     }
*/




/*
 * increas the value one
 */
/*
public void increaseValue()
{

}*/
/*
 * increase the key one
 */
/*
public void increaseKey()
{
	
}*/

