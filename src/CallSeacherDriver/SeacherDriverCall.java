package CallSeacherDriver;
import org.joy.crawler.*;
import org.joy.index.*;


public class SeacherDriverCall {
	public static void main(String[] args) {
		 String [] aa={"seeds.txt","2"};
		 int res;
			Crawler crawler= new Crawler();
			DataCleanerDriver dataCleanerDriver=new DataCleanerDriver();
			IndexConstructorDriver  indexConstructorDriver=new IndexConstructorDriver();
			try{
			res=crawler.setRun(aa);
			res=dataCleanerDriver.setRun();
			res=indexConstructorDriver.setRun();
	
			System.exit(res);
			}catch(Exception e){System.out.print(e.getMessage());}
			
			//System.exit(1);
	}
}




































































































/*
 * 
 * 		InvertedListWritable[] ilist=new InvertedListWritable[6];
			IndexNode<ListInteger,ListInteger> ls;
			for(int i=0;i<2;i++)
				ilist[i]=new InvertedListWritable();
			int k=9;
			int ml=14;
	          ListInteger li;
	          ListInteger a;
	//	for(int i=5;i>=0;i--)
		//	{
//buildi	 Number of Nodes: 5 index 0: Key:5 Value:2      index 1: Key:17 Value:2      index 2: Key:4 Value:1      index 3: Key:23 Value:1      index 4: Key:20 Value:1   


			     
				li=new ListInteger(5);
				 a=new ListInteger(2);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[0].append(ls);
				//System.out.print(a.get()+" ");
				
				li=new ListInteger(17);
				 a=new ListInteger(2);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[0].append(ls);
				//System.out.print(a.get()+" ");
				
				li=new ListInteger(4);
				 a=new ListInteger(1);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[0].append(ls);
				//System.out.print(a.get()+" ");
				
				li=new ListInteger(23);
				 a=new ListInteger(1);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[0].append(ls);
				//System.out.print(a.get()+" ");
				
				li=new ListInteger(20);
				 a=new ListInteger(1);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[0].append(ls);
				
				
				
// Number of Nodes: 7 index 0: Key:21 Value:3      index 1: Key:4 Value:2      index 2: Key:6 Value:2      index 3: Key:5 Value:2      index 4: Key:12 Value:1      index 5: Key:23 Value:1      index 6: Key:16 Value:1      
			     
				li=new ListInteger(21);
				 a=new ListInteger(3);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[1].append(ls);
				
				li=new ListInteger(4);
				 a=new ListInteger(2);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[1].append(ls);
				
				li=new ListInteger(6);
				 a=new ListInteger(2);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[1].append(ls);
				
				li=new ListInteger(5);
				 a=new ListInteger(2);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[1].append(ls);
				
				
				li=new ListInteger(12);
				 a=new ListInteger(1);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[1].append(ls);
				
				li=new ListInteger(23);
				 a=new ListInteger(1);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[1].append(ls);
				
				li=new ListInteger(16);
				 a=new ListInteger(1);
				ls=new IndexNode();
				ls.setKey(li);
				ls.setValue(a);
				ilist[1].append(ls);
				
				// li=new ListInteger(10);
				//  a=new ListInteger(new Integer(li.get()+23));
				//	ilist[i].set(li,a);
				//	System.out.print(a.get()+" ");
				
				// li=new ListInteger(23+k*ml);
				//  a=new ListInteger(new Integer(li.get()+1));
				//	ilist[i].set(li,a);
					//System.out.print(a.get()+" ");
					//k+=2;
					//ml--;
		   
		//   }
		
		//for(int i=0;i<2;i++)
		//{
		/*	ListInteger li=new ListInteger(26+k*ml);
			ListInteger a=new ListInteger(new Integer(li.get()+8));
			ls=new IndexNode(li,a);
			ilist[i].set(li,a);
			System.out.print(a.get()+" ");
			
			 li=new ListInteger(19);
			  a=new ListInteger(new Integer(li.get()+23));
				ilist[i].set(li,a);
				System.out.print(a.get()+" ");
			
			 li=new ListInteger(29+k*ml);
			  a=new ListInteger(new Integer(li.get()+1));
				ilist[i].set(li,a);
				System.out.print(a.get()+" ");
				k+=2;
				ml--;
		}
		*/
		/*for(int i=0;i<9;i++)
		{

			ListInteger li=new ListInteger(26+k*ml);
			ListInteger a=new ListInteger(new Integer(li.get()+8));
			ls=new IndexNode(li,a);
			 li=new ListInteger(29+k*ml);
			  a=new ListInteger(new Integer(li.get()+1));
				ilist[4].set(li,a);
				System.out.print(a.get()+" ");
				k+=2;
				
	//	}*/
		/*for(int i=0;i<2;i++){
			ilist[i].quickSortNodeKey();
		}
		InvertedListWritable m=new InvertedListWritable();
		m.mergeLapList(ilist, 2);
		System.out.print(m.toString()+"\n");
		m.quickSortNodeValue();
		System.out.print(m.toString());
			//}
		//	ListString re;
		//	ilist.quickSortNodeValue();
		//	ArrayList<IndexNode<ListString,ListInteger>> svk=new ArrayList<IndexNode<ListString,ListInteger>>();
			
		//	ArrayList<IndexNode<ListInteger,ListInteger >> stem=new ArrayList<IndexNode<ListInteger,ListInteger>>();
		//	stem=ilist.getArrayList();
		//	svk=ilist.swapKeyValueToVK(stem);
		//	ilist.quickSortNodeValue();
		//	stem=ilist.getArrayList();
			//for(int i=0;i<12;i++)
			//{
			//	ListInteger re=stem.get(i).getValue();
			//	System.out.print("++ "+re.get().toString()+" ");
		//	}
 * 
 * 
 * 
 * 
 * 
 * 
 * */

