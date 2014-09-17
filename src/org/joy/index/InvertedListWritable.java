package org.joy.index;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.math.*;
import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.joy.index.*;
/*
 * this class define a data structure; InvertedList, it use the generic class, the IndexNode can be 
 * define as any types
 * @author: Zhou jingbo
 * 
 */

public class InvertedListWritable  implements Writable, Cloneable {
	@SuppressWarnings("unchecked")  	
	private ArrayList<IndexNode<ListURL,ListInteger>> arrayList;

	/**
	 * construct the array list
	 */
	public InvertedListWritable() {
		arrayList= new ArrayList<IndexNode<ListURL,ListInteger>>(10);
	}
	
	public InvertedListWritable(int length){
		arrayList= new ArrayList<IndexNode<ListURL,ListInteger>>(length);
	}

	/*
	 * return the index of key
	 */
	public int indexOfKey(ListURL key) {

    	//test whether the index of object is larger than 0

	if (key == null) {

	    for (int i = 0; i < arrayList.size(); i++)//attention, this size mean the number of object in the arrayList, not the length of array

		if (arrayList.get(i).getKey()==null)

		    return i;

	} else {
		
	    for (int i = 0; i < arrayList.size(); i++)

		if (key.equals(arrayList.get(i).getKey()))//equals

		    return i;

	}

	return -1;//if failed, return -1

    }
	
	
	/**
	 * test whether the key is in the inverted list
	 * 
	 * @return the index of key
	 */
	public boolean containsKey(ListURL key) {
		return indexOfKey(key)>=0;
	}
    /*
     * append one indexnode to the end of the arrayList
     */
	public boolean append(IndexNode<ListURL,ListInteger> o){
		if(!arrayList.contains(o))
		{arrayList.add(o);
		return true;
		}
		return false;
	}
	
	
	 /*
     * append value and data e to the end of the arrayList
     */
	public void set(ListURL k,ListInteger v){
		int j=indexOfKey(k);
		if(j>=0) arrayList.get(j).setValue(v);
		if(j<0) {
			IndexNode<ListURL,ListInteger>  d=new IndexNode<ListURL,ListInteger>();
			d.setKey(k);
			d.setValue(v);
			arrayList.add(d);		
		}
		}
	/*
	 * input: int i(it is the index of arraylist), ListInterger v( it is the value of the IndexNode)
	 * output: none
	 * function: find the node with index i,(or the ith node in the arraylist), set the value of this node is v
	 * @authors:Zhou Jingbo 
	 */
	public void set(int i, ListInteger v){
		IndexNode<ListURL,ListInteger> d=new IndexNode<ListURL,ListInteger>();
		d=arrayList.get(i);
		d.setValue(v);
		arrayList.set(i, d);
	}
	/*
	 * set the array list with a new arraylist
	 */
	public void setArrayList(ArrayList<IndexNode<ListURL,ListInteger>> newList){
		this.arrayList=newList;
	}
	
	/*
	 * get the value by index
	 */
	public ListInteger getValue(int i){
		return arrayList.get(i).getValue();
	}

	/*
	 * get the key by index
	 */
	public ListURL getKey(int i){
		return arrayList.get(i).getKey();
	}
	/*
	 * get the value by key
	 */
	public ListInteger getValue(ListURL k){
		return arrayList.get(indexOfKey(k)).getValue();
	}
	
	/*
	 * get the indexnode by index
	 */
	public IndexNode<ListURL,ListInteger> getIndexNode(int i){
		return arrayList.get(i);
	}
	
	/*
	 * get the node according to the Key
	 */
	public IndexNode<ListURL,ListInteger> getIndexNode(ListURL k){
		return arrayList.get(indexOfKey(k));
	}
	/*
	 * get the length of the list( the number of nodes)
	 */
	public int getLength(){
		return arrayList.size();
	}
	
	/*
	 * Input: ListInter key
	 * Output: no
	 * function:
	 * if the key exists in the arrayList, let its value increase 1. 
	 * if key does not exits in the arrayList, append a node with this key to the arrayLis
	 * this function is rather useful in the reduce phase for the generation of inverted list;
	 * @author:Zhou Jingbo
	 */
	public void paddingValueKey(ListURL key){
			int i=indexOfKey(key);
			int d=1;
			if(i>=0){
				ListInteger value=arrayList.get(i).getValue();
				value.set(value.get()+d);
				set(i,value);
			}
			else{
				ListInteger newValue=new ListInteger(d);
				IndexNode<ListURL,ListInteger> newNode=new IndexNode<ListURL,ListInteger>(key,newValue);
				arrayList.add(newNode);
			}
		}
	

	/*
	 * return the arrayList
	 */
	public ArrayList<IndexNode<ListURL,ListInteger>> getArrayList(){
			return arrayList;
	}
	
	
	
	/*
	 * the I/O for hadoop 
	 */
@Override
	public void readFields(DataInput in) throws IOException {
			ListURL listK;
			ListInteger listV;
			
			int n;
			int v=0;
			String con="";
			IndexNode<ListURL,ListInteger> temNode;
			int len=in.readInt();//read the len of array
			arrayList.clear();
			arrayList.ensureCapacity(len);//keep the capacity is enough for the insertion
			for(int i=0;i<len;i++)
			{		listK=new ListURL();
	 		
					//read url
					n= in.readInt();
					byte[] b = new byte[n];
					in.readFully(b);
					con = new String(b, "utf-8");
					listK.setURL(con);
			
					//read abstract
					n= in.readInt();
					b = new byte[n];
					in.readFully(b);
					con = new String(b, "utf-8");
					listK.setAbstract(con);
			
					//read the priority
					v=in.readInt();
					listV=new ListInteger(v);
					temNode =new IndexNode<ListURL,ListInteger>(listK,listV);
					arrayList.add(temNode);		 
			}	
	}
/*
 * 
 * the standard I/O for hadoop
 * */
@Override
	public void write(DataOutput out) throws IOException {
			int len=arrayList.size();
			IndexNode<ListURL,ListInteger> tem;
			String con="";
			out.writeInt(len);
			for(int i=0;i<len;i++)
			{  //write out the content(URL,Abstract)
				//first ,write the URL
				tem=arrayList.get(i);
				con=tem.getKey().getURL();
				byte[] b = con.getBytes("utf-8");
				out.writeInt(b.length);
				out.write(b);
				//second, write the abstract
				con=tem.getKey().getabs();
				byte[] c  = con.getBytes("utf-8");
				out.writeInt(c.length);
				out.write(c);
				//the priority
				out.writeInt(tem.getValue().get());
			//	out.writeInt(1);*/
			}
	  
	}
/*
 * 
 * 
 * (non-Javadoc)
 * @see java.lang.Object#toString()
 */
@Override
	public String toString() {
		
		   String list="";
		   int len=arrayList.size();
		   ListURL keyFile[]=new ListURL[len];
		   int valueFile[]=new int[len];
		   IndexNode<ListURL,ListInteger> tem=new IndexNode<ListURL,ListInteger>();
		   for(int i=0;i<len;i++)
		   {
			  tem=arrayList.get(i);
			  keyFile[i]=tem.getKey();
			  valueFile[i]=tem.getValue().get();
		   }
		   		list+="total Number of URL/docs is: "+len+" \n";
		   for(int i=0;i<len;i++){
			   list+="No."+i+":"+"	URL/doc_id: "+keyFile[i].getURL()+"	   Ranking Priority: "+valueFile[i]+"\n";
			   list+="	    the abstract is: \n";
			   list+="	    "+keyFile[i].getabs()+"\n\n";
		   }
		   return list;
		}
/*
 * function:
 * clone, deep clone,be careful, in some case, deep clone is necessary
 * @author:Zhou jingbo
 * (non-Javadoc)
 * @see java.lang.Object#clone()
 */
@Override
	public Object clone(){
		InvertedListWritable o = null;
		try{
			o = (InvertedListWritable)super.clone();
			}catch(CloneNotSupportedException e){
				e.printStackTrace();
			}
			o.arrayList= (ArrayList<IndexNode<ListURL,ListInteger>>)arrayList.clone();
			return o;
		}

	/*
	 * quick sort, sort the data in the list according to the value, decrease
	 * @author:Zhou jingbo
	*/
	public  void quickSortNodeValue()
      {
		findPartitionValue(arrayList,0,this.arrayList.size());
      }
	/*
	 *quick sort, sort the data in the list according to the Key, decrease
	 *@author:Zhou jingbo
	 */
    public void quickSortNodeKey(){
    	findPartitionKey(arrayList,0,this.arrayList.size());
    }
	
	 /*
	  * quick sort function 
	  * @author:Zhou Jingbo
	 */
    public void findPartitionValue(ArrayList<IndexNode<ListURL,ListInteger>> data, int min,int max){
              int left ,right;
              IndexNode<ListURL,ListInteger> temp,partitionelement;
              
              partitionelement = data.get(min);//data[min];
              
              left = min;// from the first of the left 
              right = max;// from the right of the left
 
              
              while(true){//until go to the middle of the two data
                      
                      //search the data larger than partition element from left to right
                      //search for an element that is > the partition element
                      while((++left)<max-1&&(data.get(left).comToValue(partitionelement) >0));
              
                      //search for an element that is < the partition element
                      while ((--right)>min && (data.get(right).comToValue(partitionelement) < 0 ));
                      if(left>=right) break;
                      //exchange the index
                     
                      temp = data.get(left);
                      data.set(left,data.get(right));
                      data.set(right,temp);
              }

              data.set(min,data.get(right));
              data.set(right,partitionelement);
              
              if(min<right) 
            	  findPartitionValue(data, min,right);
              
              if(max>left)
            	  findPartitionValue(data,left,max);
      }
    
    /*
	  * quick sort function according to key
	  * autthor:Zhou Jingbo
	 */
   public void findPartitionKey(ArrayList<IndexNode<ListURL,ListInteger>> data, int min,int max){
             int left ,right;
             IndexNode<ListURL,ListInteger> temp , partitionelement;
             
             partitionelement = data.get(min);//data[min];
             
             left = min;// from the first of the left 
             right = max;// from the right of the left

             
             while(true){//until go to the middle of the two data
                     
                     //search the data larger than partition element from left to right
                     //search for an element that is > the partition element
                     while((++left)<max-1&&(data.get(left).compareTo(partitionelement) >0));
             
                     //search for an element that is < the partition element
                     while ((--right)>min && (data.get(right).compareTo(partitionelement) < 0 ));
                     if(left>=right) break;
                     //exchange the index
                    
                     temp = data.get(left);
                     data.set(left,data.get(right));
                     data.set(right,temp);
             }

             data.set(min,data.get(right));
             data.set(right,partitionelement);
             
             if(min<right) 
           	  findPartitionKey(data, min,right);
             
             if(max>left)
           	  findPartitionKey(data,left,max);
     }
   
   
/*
 * input: a array of InvertedListWritable
 * output: no
 * them merge result is stored in the arrayList of the
 *  InvertedListWritable, the local arrayList is ignore. 
 * merge the list according to the decrease order
 * @author:Zhou Jingbo
 */
   	
   	public void mergeLapList(InvertedListWritable[] sortedInverted,int n){
   			ArrayList<IndexNode<ListURL,ListInteger>> sorted =new ArrayList<IndexNode<ListURL,ListInteger>>();
   			int sumLen=0;
   			int sortPointer=0; 				//the global pointer for sorting
   			int invertedLen[]=new int[n];	//record the length of every invertedlist
   			int invertedPointer[]=new int[n];//record the pointer of very invertedlist during the merging process
   			ListURL lk;					//the key of temp
   			ListInteger lv;					//the value of temp
   			IndexNode<ListURL,ListInteger> temp; 	//the temp
   			IndexNode<ListURL,ListInteger> mid;		//the mid. temp and mid need to be compared
   			int d;				//the difference of two indexNode, the return vaule of compareTo
   			int lap=0;			//the number of node have the same key
   			int lapRecord[]=new int[n];		//the number of the sortedInvertedlist with the same node
   			ListInteger lapValue[]=new ListInteger[n];	// record the value of the node with same key
   			String absR="";
   			String urlR="";
   			
   			ListURL ab;
   			int sumValue=0;
   			for(int i=0;i<n;i++)
   			{
   				sumLen+=sortedInverted[i].getLength();
   				invertedLen[i]=sortedInverted[i].getLength();
   				invertedPointer[i]=0;
   				lapRecord[i]=-1;
   				lap=-1;
   				d=0;
   				lapValue[i]=new ListInteger(0);
   			}
   			sorted.clear();					//clear the arrayList
   			sorted.ensureCapacity(sumLen);	// let space is enough
   			while(sortPointer<sumLen){
   			lk=new ListURL();
   			lv=new ListInteger(0);
   			temp=new IndexNode();
   			temp.setKey(lk);
   			temp.setValue(lv);
   			//mid=new IndexNode(lk,lv);
   			sumValue=0;
   			lap=-1;
   			d=0;
   			for(int i=0;i<n;i++){
   				if(invertedPointer[i]<invertedLen[i]){
   					mid=sortedInverted[i].getIndexNode(invertedPointer[i]);
   					d=temp.compareTo(mid);
   					if(d<0){
   						temp.setKey(mid.getKey());
   						lap=0;
   						lapRecord[lap]=i;
   						lapValue[lap].set(mid.getValue().get());
   					}
   					else if(d==0){
   						lap++;
   						lapRecord[lap]=i;
   						lapValue[lap].set(mid.getValue().get());
   						absR=mid.getKey().getabs();
   						absR=temp.getKey().getabs()+"......"+absR;
   						temp.getKey().setAbstract(absR);   						
   					}
   				}
   			}//end for(i=0;
   			temp.setValue(new ListInteger(0));
   			for(int j=0;j<=lap;j++){
   				sumValue+=lapValue[lap].get();
   				invertedPointer[lapRecord[j]]++;
   			}//end for (j=0;
   			temp.setValue(new ListInteger((int)(sumValue*(Math.log10(2+lap)/Math.log10(2))))); //the more lap, the higher priority
   			System.out.print("\n the lap is "+lap+"  \n");
   			sorted.add(temp);
   			sortPointer+=(lap+1);
   				
   		}
   		arrayList=sorted;
   	}

}

































