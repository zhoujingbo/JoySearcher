package org.joy.crawler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.Writable;

/**
 * Hadoop writable class for storing outlnks structure. <br/>
 * NOTE: this class can also used for storing the HTTP redirect relation, whose
 * outlinks array is a one-element array and redirect flag is true.
 * 
 * @author Song Liu (Lamfeeling@126.com)
 * 
 */
public class OutlinksWritable implements Writable, Comparable<OutlinksWritable> {
    private String[] outlinks = new String[0];
    public final static int REDIRECTED = 1, NORMAL = 0, REMOVE_NORMAL = 4;
    private int typeOfOutlink = NORMAL;
    private long timeStamp = Calendar.getInstance().getTimeInMillis();

    public OutlinksWritable() {

    }

    /**
     * construct an outlinkswritable using outlinks String array.
     * 
     * @param outlinks
     *            a String array contains all the outlinks.
     * @param type
     *            the type of outlink. could be normal, redirected, or
     *            duplicated
     */
    public OutlinksWritable(String[] outlinks, int type) {
	super();
	this.outlinks = outlinks;
	this.typeOfOutlink = type;
    }

    public OutlinksWritable(OutlinksWritable o){
	this.outlinks = o.outlinks;
	this.typeOfOutlink = o.typeOfOutlink;
	this.timeStamp = o.timeStamp;
    }
    /**
     * get outlinks array from this object
     * 
     * @return outlinks array from this object
     */
    public String[] getOutlinks() {
	return outlinks;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	// TODO Auto-generated method stub
	int size = in.readInt();
	outlinks = new String[size];
	for (int i = 0; i < outlinks.length; i++) {
	    outlinks[i] = in.readUTF();
	}
	typeOfOutlink = in.readInt();
	timeStamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	// TODO Auto-generated method stub
	out.writeInt(outlinks.length);
	for (String s : outlinks) {
	    out.writeUTF(s);
	}
	out.writeInt(typeOfOutlink);
	out.writeLong(timeStamp);
    }

    /**
     * return the type of outlink
     * 
     * @returnthe type of outlink
     */
    public int getTypeOfOutlink() {
	return typeOfOutlink;
    }

    public long getTimeStamp() {
	return timeStamp;
    }

    @Override
    public String toString() {
	String res = typeOfOutlink + "\t";
	for (String s : outlinks) {
	    res += s + "\t";
	}
	return res;
    }

    @Override
    public int compareTo(OutlinksWritable o) {
	// TODO Auto-generated method stub
	return (int) (this.getTimeStamp() -o.getTimeStamp() );
    }

}
