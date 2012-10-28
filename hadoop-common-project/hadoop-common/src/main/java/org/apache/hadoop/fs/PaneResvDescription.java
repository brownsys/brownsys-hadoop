package org.apache.hadoop.fs;

import edu.brown.cs.paneclient.*;

public class PaneResvDescription {
	//instead of using a PaneReservation object,
	//this class is used for passing information
	PaneTime start = null;
	PaneTime end = null;
	String principal = null;
	String share = null;
	int bandwidth = -1;
	PaneFlowGroup flowgroup = null;
	long size = 0;
	
	public PaneResvDescription() {
		flowgroup = new PaneFlowGroup();
	}

	public void setStart(PaneTime start) {
		this.start = start;
	}

	public void setEnd(PaneTime end) {
		this.end = end;
	}

	public void setUser(String principal) {
		this.principal = principal;
	}

	public void setShare(String share) {
		this.share = share;
	}

	public void setBandwidth(int bandwidth) {
		this.bandwidth = bandwidth;
	}

	public void setFlowGroup(PaneFlowGroup flowgroup) {
		this.flowgroup = flowgroup;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public PaneTime getStart(){
		return this.start;
	}

	public PaneTime getEnd(){
		return this.end;
	}

	public String getUser() {
		return this.principal;
	}

	public String getShare() {
		return this.share;
	}

	public int getBandwidth() {
		return this.bandwidth;
	}

	public PaneFlowGroup getFlowGroup() {
		return this.flowgroup;
	}
	
	public long getSize() {
		return this.size;
	}

	public String toString() {
		String string = "Resv: start:" + this.start + " end:" + this.end + " share:" + this.share +
				" user:" + this.principal + " bandwidth:" + this.bandwidth;
		if (this.flowgroup == null)
			string += " fg:null";
		else
			string += this.flowgroup.generateConfig();
		return string;
	}
}
