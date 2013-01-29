package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.brown.cs.paneclient.*;
import edu.brown.cs.paneclient.PaneException.InvalidResvException;

public class PaneSpeakerShuffle {

	InetSocketAddress paneAddress;

	int maxBandwidth;
	PaneShare share;
	int selfPort;

	static int baseRate = 1;

	private static final Log LOG = LogFactory.getLog(PaneSpeakerShuffle.class);

	public PaneSpeakerShuffle(InetSocketAddress paneAddress, PaneShare share) {
		this.paneAddress = paneAddress;
		this.share = share;
	}

	private int computePaneRate(int time, long size) {
		
		double result = (double)size/(1024*1024)/(double)time;//MB per second
		int rate = (int)Math.round(result);
        rate *= 8; // convert bytes to bits

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
		int rate = computePaneRate(deadline, size);
		start.setRelativeTime(0);
		long endtime = deadline + 4; // give some grace time
		if (rate == baseRate) {
			//probably deadline is too large for this small flow
			endtime = size/(1024*1024)/rate;
			if(endtime == 0) {
				//otherwise it would from now to now and cause error,
				//probably does not need to reserve in this case?
				endtime = 1;
			}
		}
		end.setRelativeTime(endtime);

		fg = new PaneFlowGroup();
		fg.setTransportProto(PaneFlowGroup.PROTO_TCP);

		//from EventFether, host name of MapHost is constructed by "u.getHost() + ":" + u.getPort()" 

		String[] addr = host.getHostName().split(":");
		int srcPort = Integer.parseInt(addr[addr.length - 1]);
		fg.setSrcPort(srcPort);

		InetAddress srcHost = InetAddress.getByName(addr[0]);
		InetAddress localHost = InetAddress.getLocalHost();
		InetAddress src = fg.getSrcHost();
		if (srcHost.equals(localHost)) {
			LOG.info("The shuffle reservation has same dst and src host, ignore");
			return true;
		}

		fg.setSrcHost(srcHost);

		fg.setDstPort(myPort);
		fg.setDstHost(localHost);
		resv = new PaneReservation(computePaneRate(deadline, size), fg, start, end);

		try {
			share.reserve(resv);
		} catch (InvalidResvException e) {
			LOG.error("Failed to make PANE reservation, " + e);
			return false;
		} catch (SocketException e) {
			LOG.error("SocketException when making PANE reservation, " + e);
			return false;
		}

		LOG.info("Making PANE reservation succeeded:" + resv.generateCmd());
		return true;
	}
}
