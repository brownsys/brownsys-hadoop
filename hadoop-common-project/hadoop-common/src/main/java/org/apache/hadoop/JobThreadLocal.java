package org.apache.hadoop;

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

}
