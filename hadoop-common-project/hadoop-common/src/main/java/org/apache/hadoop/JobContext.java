package org.apache.hadoop;

public class JobContext {

	private String jobId = null;
	
    public void setJobId(String id) {
        this.jobId = id;
    }

    public String getJobId() {
        return this.jobId;
    }
}
