package mapreduce;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.math.RandomUtils;

/**
 * <h1>Job class</h1>
 * <h3>A class which is used to send the map jobs to the slave nodes</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Job implements java.io.Serializable {
	Integer jobId;
	List<String> fileList;

    /***
     * The constructor used to create a map Job with a list of files
     * @param fileArr The list of files to be set to the Job
     */
	public Job(List<String> fileArr) {
		this.jobId = RandomUtils.nextInt();
		this.fileList = fileArr;
	}

    /***
     * Returns the string of the Job object
     * @return string of the object.
     */
	@Override
	public String toString() {
		return "Job [jobId=" + jobId + ", fileArr=" + fileList
				+ "]";
	}
	
}
