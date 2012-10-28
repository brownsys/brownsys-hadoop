package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.brown.cs.paneclient.*;
import edu.brown.cs.paneclient.PaneException.InvalidResvException;

public class PaneSpeakerShuffle {

	InetSocketAddress paneAddress;

	int maxBandwidth;
	PaneShare share;
	int selfPort;

	static int baseRate = 100;

	private static final Log LOG = LogFactory.getLog(PaneSpeakerShuffle.class);

	public PaneSpeakerShuffle(InetSocketAddress paneAddress, PaneShare share) {
		this.paneAddress = paneAddress;
		this.share = share;
	}

	private int computePaneRate(int time, long size) {
		
		double result = (double)size/(double)time;
		int rate = (int)Math.round(result);

		if (rate < baseRate) {
			rate = baseRate;
		}

		return rate;
	}

	public boolean makeReservation(MapHost host, long size, int myPort, int deadline) throws IOException {

		LOG.info("Making PANE reservation, deadline:" + deadline + " size:" + size);

		PaneReservation resv;
		PaneFlowGroup fg;
		PaneRelativeTime start = new PaneRelativeTime();
		PaneRelativeTime end = new PaneRelativeTime();

		start.setRelativeTime(0);
		end.setRelativeTime(deadline);

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
		resv = new PaneReservation(computePaneRate(deadline, size), fg, start, end);

		try {
			share.reserve(resv);
		} catch (InvalidResvException e) {
			LOG.error("Failed to make PANE reservation, " + e);
			return false;
		}

		LOG.info("Making PANE reservation succeeded:" + resv.generateCmd());
		return true;
	}
}
