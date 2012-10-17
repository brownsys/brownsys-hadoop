package org.apache.hadoop.trace;

import org.apache.commons.logging.Log;
import org.slf4j.Logger;

public class TraceHadoop {

    public static String genTraceString(String jobId, String socketInfo) {
        String strace = "";
        for (StackTraceElement ste : Thread.currentThread().getStackTrace())
          strace += (" " + ste);
        String traceString = "<trace-tag> " + 
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
    
}
