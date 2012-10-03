package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.brown.cs.paneclient.*;
import edu.brown.cs.paneclient.PaneException.InvalidAuthenticateException;
import edu.brown.cs.paneclient.PaneException.InvalidNewShareException;
import edu.brown.cs.paneclient.PaneException.InvalidResvException;

public class PaneSpeaker {
	
	InetSocketAddress paneAddress;
	
	int maxBandwidth;
	PaneShare share;
        int selfPort;
	
	private static final Log LOG = LogFactory.getLog(PaneSpeaker.class);
	
	public PaneSpeaker(InetSocketAddress paneAddress, PaneShare share) {
		this.paneAddress = paneAddress;
		this.share = share;
	}
	
	private int computePaneRate(int time, long size) {
		return 10;
	}

	private int computeTime(long size) {
		return 30000;
	}
	
	public boolean makeReservation(MapHost host, long size, int myPort) throws IOException {
		
                LOG.info("Making PANE reservation");

		PaneReservation resv;
		PaneFlowGroup fg;
		PaneRelativeTime start = new PaneRelativeTime();
		PaneRelativeTime end = new PaneRelativeTime();
		
		start.setRelativeTime(0);
		int time = computeTime(size);
		end.setRelativeTime(time);
		
		fg = new PaneFlowGroup();
                fg.setTransportProto(PaneFlowGroup.PROTO_TCP);
		
		//from EventFether, host name of MapHost is constructed by "u.getHost() + ":" + u.getPort()" 
		
		String[] addr = host.getHostName().split(":");
		int srcPort = Integer.parseInt(addr[addr.length - 1]);
		fg.setSrcPort(srcPort);
		
		InetAddress srcHost = InetAddress.getByName(addr[0]);
		fg.setSrcHost(srcHost);
		
                fg.setDstPort(myPort);
		fg.setDstHost(InetAddress.getLocalHost());
		resv = new PaneReservation(computePaneRate(time, size), fg, start, end);
		
		try {
			share.reserve(resv);
		} catch (InvalidResvException e) {
			LOG.error("Failed to make PANE reservation, " + e);
			return false;
		}

                LOG.info("Making PANE reservation succeeded");
                return true;
	}
}
