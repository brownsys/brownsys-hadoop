package org.apache.hadoop;

import org.apache.commons.logging.Log;

public class JobThreadLocal {
	public static final ThreadLocal<JobContext> userThreadLocal = new ThreadLocal<JobContext>();

    public static void set(JobContext user) {
    	userThreadLocal.set(user);
    }

    public static void unset() {
    	userThreadLocal.remove();
    }

    public static JobContext get() {
		return userThreadLocal.get();
    }

    public static String getJobId() {
  	  JobContext ctx = userThreadLocal.get();
  	  String jobId = "UNKNOWN";
  	  if (ctx != null && ctx.getJobId() != null)
  		  jobId = ctx.getJobId();
      return jobId;
    }
    
    public static void logTrace(Log logger, String className, String socketInfo) {
        String strace = "";
        for (StackTraceElement ste : Thread.currentThread().getStackTrace())
          strace += (" " + ste);
        
    	logger.info("<trace-tag> " + className + ", " + 
    			"JobID: " + getJobId() + ", " + 
    			"ThreadName: " + Thread.currentThread().getName() + ", " + 
    			socketInfo + ", " + 
    			"due to stack: " + strace);
    }

}
