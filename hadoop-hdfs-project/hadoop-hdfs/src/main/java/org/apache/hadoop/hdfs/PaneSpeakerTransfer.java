package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import edu.brown.cs.paneclient.*;
import edu.brown.cs.paneclient.PaneException.InvalidAuthenticateException;
import edu.brown.cs.paneclient.PaneException.InvalidResvException;

public class PaneSpeakerTransfer {

	InetSocketAddress paneAddress;
	PaneShare share;
	int selfPort;
	PaneClientImpl paneClient;
	static int baseRate = 1;
	
	public void initialize(String paneHost, int panePort, String paneShareName, String paneUser) {
    	DFSClient.LOG.info("PANE info: Address:" + paneHost + " Port:" + panePort + " sharename:" + paneShareName
    			+ " user:" + paneUser);

    	try {
    		paneAddress = new InetSocketAddress(InetAddress.getByName(paneHost), panePort);
    	} catch (UnknownHostException e) {
    		DFSClient.LOG.error("Unknown PANE host, " + e);
    		return;
    	}

    	try {
    		paneClient = new PaneClientImpl(paneAddress.getAddress(), paneAddress.getPort());
    	} catch (IOException e) {
    		DFSClient.LOG.error("Failed to create PANE client, from " + paneAddress + " " + e);
    		return;
    	}

    	DFSClient.LOG.info("PANE Client created");

    	this.share = new PaneShare(paneShareName, Integer.MAX_VALUE, null);
    	this.share.setClient(paneClient);

    	DFSClient.LOG.info("PANE share name:" + paneShareName);

    	try {		  
    		paneClient.authenticate(paneUser);
    	} catch (InvalidAuthenticateException e) {
    		DFSClient.LOG.error("Invalid authentication of PANE client, " + e);
    		return;
    	} catch (IOException e) {
    		DFSClient.LOG.error("Failed to create new share, " + e);
    		return;
    	}

    	DFSClient.LOG.info("PANE authentication succeeded, user name:" + paneUser);
    	DFSClient.LOG.info("PANE initialization completed");
	}

	private int computePaneRate(int time, long size) {
		double result = (double)size/1024*1024/(double)time;
		int rate = (int)Math.round(result);
        rate *= 8; // convert bytes to bits
		if (rate < baseRate) {
			rate = baseRate;
		}
		return rate;
	}

	public boolean makeReservation(PaneFlowGroup fg, long size, int deadline) throws IOException {
		PaneRelativeTime start = new PaneRelativeTime();
		PaneRelativeTime end = new PaneRelativeTime();
		InetAddress dst = fg.getDstHost();
		InetAddress src = fg.getSrcHost();
		if (dst.equals(src)) {
			DFSClient.LOG.info("The transfer reservation has same dst and src host, ignore");
			return true;
		}

		int rate = computePaneRate(deadline, size);
		start.setRelativeTime(0);
		long endtime = deadline + 4; // give some grace time
		if (rate == baseRate) {
			//probably deadline is too large for this small flow
			endtime = size/1024*1024/rate;
			if(endtime == 0) {
				//otherwise it would from now to now and cause error,
				//probably does not need to reserve in this case?
				endtime = 1;
			}
		}		
		end.setRelativeTime(endtime);
		fg.setTransportProto(PaneFlowGroup.PROTO_TCP);
		PaneReservation resv = new PaneReservation(computePaneRate(deadline, size), fg, start, end);
		try {
			//share == null means initialization failed!
			if (share != null)
				share.reserve(resv);
		} catch (InvalidResvException e) {
			DFSClient.LOG.error("Failed to make PANE reservation, " + e);
			return false;
		} catch (SocketException e) {
			DFSClient.LOG.error("SocketException when making PANE reservation, " + e);
			return false;
		}

		DFSClient.LOG.info("Making PANE reservation succeeded:" + resv.generateCmd());
		return true;
	}
	
	public boolean makeReservation(InetAddress srcHost, int srcPort, InetAddress dstHost, int dstPort, int size, int deadline) throws IOException {

		PaneFlowGroup fg;
		fg = new PaneFlowGroup();
		fg.setTransportProto(PaneFlowGroup.PROTO_TCP);
		
		fg.setSrcHost(srcHost);
		fg.setDstHost(dstHost);
		
		fg.setSrcPort(srcPort);
		fg.setDstPort(dstPort);
		
		return makeReservation(fg, size, deadline);
	}
}

