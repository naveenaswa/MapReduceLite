package mapreduce;

import java.util.List;

import org.apache.commons.lang.math.RandomUtils;

/**
 * <h1>JobReduce class</h1>
 * <h3>A class which is used to send the reduce jobs to the slave nodes</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class JobReduce implements java.io.Serializable {
	Integer jobId;
	List<String> keys;

	/***
	 * The constructor used to create a reduce Job with a list of keys
	 * @param keys The list of keys to be set to the Job
	 */
	public JobReduce(List<String> keys) {
		this.jobId = RandomUtils.nextInt();
		this.keys = keys;
	}

    /***
     * Returns the string of the JobReduce object
     * @return string of the object.
     */
	@Override
	public String toString() {
		return "JobReduce [jobId=" + jobId + ", keys=" + keys + "]";
	}	
	
	
}