package org.apache.hadoop.trace;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.json.simple.JSONObject;

public class TraceHadoop {

	public static String getHostName(SocketAddress addr) {
		return (addr instanceof InetSocketAddress) ? ((InetSocketAddress)addr).getHostName() : addr.toString();
	}
	
	public static Integer getPort(SocketAddress addr) {
		return (addr instanceof InetSocketAddress) ? new Integer(((InetSocketAddress)addr).getPort()) : null;
	}
	
    public static String genTraceString(String jobId, String socketInfo) {
        String strace = "";
        for (StackTraceElement ste : Thread.currentThread().getStackTrace())
          strace += (" " + ste);
        String traceString = "<deprecated-tag> " + 
    			"JobID: " + jobId + ", " + 
    			"ThreadName: " + Thread.currentThread().getName() + ", " + 
    			socketInfo + ", " + 
    			"due to stack: " + strace;
        return traceString;
    }
	
    public static void logTrace(Log logger, String moreInfo) {
    	logger.info(genTraceString(JobThreadLocal.getJobId(), moreInfo));
    }
    
    public static void logTrace(Logger logger, String moreInfo) {
    	logger.info(genTraceString(JobThreadLocal.getJobId(), moreInfo));
    }
    
    public static void logTrace(Log logger, String jobId, String socketInfo) {
    	logger.info(genTraceString(jobId, socketInfo));
    }
    
    private static String formatAddress(String addr) {
    	addr = addr.replaceAll("/", "");
    	addr = addr.replaceAll(".cs.brown.edu", "");
    	return addr;
    }
    
    @SuppressWarnings("unchecked")
	public static String genTraceObjStr(String jobId, String src, Integer src_port, String dest, Integer dest_port) {
    	JSONObject obj = new JSONObject();
        String strace = "";
        for (StackTraceElement ste : Thread.currentThread().getStackTrace())
          strace += (" " + ste);
        
        obj.put("id", "trace-tag");
        obj.put("timestamp", new Long(System.currentTimeMillis()));
        obj.put("jobid", jobId);
    	obj.put("threadname", Thread.currentThread().getName());
        obj.put("source", formatAddress(src));
        obj.put("source_port", src_port);
        obj.put("dest", formatAddress(dest));
        obj.put("dest_port", dest_port);
    	obj.put("stacktrace", strace);
    	if (src_port == null || dest_port == null)
    		obj.put("single_address", true);
    	else
    		obj.put("single_address", false);
    	return obj.toString();
    }

    public static void logTrace(Logger logger, String src, Integer src_port, String dest, Integer dest_port) {
    	logger.info(genTraceObjStr(JobThreadLocal.getJobId(), src, src_port, dest, dest_port));
    }    

	public static void logTrace(Log logger, String src, Integer src_port, String dest, Integer dest_port) {
    	logger.info(genTraceObjStr(JobThreadLocal.getJobId(), src, src_port, dest, dest_port));
    }
    
    public static void logTrace(Logger logger, String jobId, String src, Integer src_port, String dest, Integer dest_port) {
    	logger.info(genTraceObjStr(jobId, src, src_port, dest, dest_port));
    }    

	public static void logTrace(Log logger, String jobId, String src, Integer src_port, String dest, Integer dest_port) {
    	logger.info(genTraceObjStr(jobId, src, src_port, dest, dest_port));
    }

}

