package mapreduce;

import java.util.List;
import com.amazonaws.services.s3.AmazonS3URI;

/**
 * <h1>Configuration Class</h1>
 * <h3>Provides access to configuration parameters.User sets the parameters to specify folder paths,
 * and class names related to mapper, reducer, keys, values, IP configuration details for AWS
 * clusters, instance count and data bucket paths.
 * </h3>
 *
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Configuration {
	private  String inputDirPath;
    private  String outputDirPath;
    private  Class<? extends Mapper> mapperClass;
    private  Class<? extends Reducer> reducerClass;
    private  Class<?> mapOutputKey;
    private  Class<?> mapOutputValue;
    private  Class<?> reduceOutputKey;
    private  Class<?> reduceOutputValue;
	private IPDetails masterDetails;
	private List<IPDetails> nodeDetails;
	private int instanceCount;
	private String s3Bucket;
	private String globalVal;

	/**
	 * <p>
	 * Get the value of global value. Can be used to store some parameters which can be used across the framework.
	 * @param none no parameter required
	 * @return <i>String</i> - value representing global value.
	 */
	public String getGlobalVal() {
		return globalVal;
	}

	/**
	 * <p>
	 * Set the value of global value
	 * @param globalVal string which is set as globalval.
	 * @return <i>void</i> - does not return any value.
	 */
	public void setGlobalVal(String globalVal) {
		this.globalVal = globalVal;
	}
	/**
	 * <p>
	 * Get the directory path value of input folder
	 * @param none no parameter required
	 * @return <i>String</i> - value representing the directory path of input folder.
	 */
	public  String getInputDirPath() {
		return inputDirPath;
	}
	/**
	 * <p>
	 * Set the directory path value of input folder
	 * @param String input directory path
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setInputDirPath(String inputDirPath) {
		this.inputDirPath = inputDirPath;		
	}
	/**
	 * <p>
	 * Get the directory path value of output folder
	 * @param none no parameter required
	 * @return <i>String</i> - value representing the directory path of output folder.
	 */
	public  String getOutputDirPath() {
		return outputDirPath;
	}
	/**
	 * <p>
	 * Set the directory path value of ouput folder. Additionally set the AWS output directory path
	 * to the amazon S3 using URI objects.
	 * @see com.amazonaws.services
	 * @param String output directory path
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setOutputDirPath(String outputDirPath) {
		this.outputDirPath = outputDirPath;
		AmazonS3URI URL1 = new AmazonS3URI(outputDirPath);			
		String bucketName = URL1.getBucket();
		this.s3Bucket = "s3://" + bucketName + "/";
	}
	/**
	 * <p>
	 * Get the type of mapper class. User extends the Mapper class and specifies the class type.
	 * @param none no parameter required
	 * @return <i>unknown class type</i> - class value representing the mapper class user specifies.
	 */
	public  Class<? extends Mapper> getMapperClass() {
		return mapperClass;
	}
	/**
	 * <p>
	 * Set the type of mapper class. User extends the Mapper class and specifies the class type.
	 * @param mapperClass unknown class type user sets during his implementation.
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}
	/**
	 * <p>
	 * Get the type of reducer class. User extends the Reducer class and specifies the class type.
	 * @param none no parameter required
	 * @return <i>unknown class type</i> - class value representing the reducer class user specifies.
	 */
	public  Class<? extends Reducer> getReducerClass() {
		return reducerClass;
	}
	/**
	 * <p>
	 * Set the type of reducer class. User extends the Reducer class and specifies the class type.
	 * @param reducerClass unknown class type user sets during his implementation.
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setReducerClass(Class<? extends Reducer> reducerClass) {
		this.reducerClass = reducerClass;
	}
	/**
	 * <p>
	 * Get the class type of mapper output key
	 * @param none no parameter required
	 * @return <i>class type</i> - class type representing the type of mapper output key.
	 */
	public  Class<?> getMapOutputKey() {
		return mapOutputKey;
	}
	/**
	 * <p>
	 * Set the class type of mapper output key as specified by user in his implementation.
	 * @param mapOutputKey unknown class type of map output key.
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setMapOutputKey(Class<?> mapOutputKey) {
		this.mapOutputKey = mapOutputKey;
	}
	/**
	 * <p>
	 * Get the class type of mapper output value
	 * @param none no parameter required
	 * @return <i>class type</i> - class type representing the type of mapper output value.
	 */
	public  <T> Class<?> getMapOutputValue() {
		return mapOutputValue;
	}
	/**
	 * <p>
	 * Set the class type of mapper output value as specified by user in his implementation.
	 * @param mapOutputValue unknown class type of map output value.
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setMapOutputValue(Class<?> mapOutputValue) {
		this.mapOutputValue = mapOutputValue;
	}
	/**
	 * <p>
	 * Get the class type of recuder output key
	 * @param none no parameter required
	 * @return <i>class type</i> - class type representing the type of reducer output key.
	 */
	public  Class<?> getReduceOutputKey() {
		return reduceOutputKey;
	}
	/**
	 * <p>
	 * Set the class type of reducer output key as specified by user in his implementation.
	 * @param reduceOutputKey unknown class type of reducer output key.
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setReduceOutputKey(Class<?> reduceOutputKey) {
		this.reduceOutputKey = reduceOutputKey;
	}
	/**
	 * <p>
	 * Get the class type of reducer output value
	 * @param none no parameter required
	 * @return <i>class type</i> - class type representing the type of reducer output value.
	 */
	public  Class<?> getReduceOutputValue() {
		return reduceOutputValue;
	}
	/**
	 * <p>
	 * Set the class type of reducer output value as specified by user in his implementation.
	 * @param reduceOutputValue unknown class type of reduce output value.
	 * @return <i>void</i> - does not return any value.
	 */
	public  void setReduceOutputValue(Class<?> reduceOutputValue) {
		this.reduceOutputValue = reduceOutputValue;
	}
	/**
	 * <p>
	 * Set the ip details of master node to the Object of type IPDetails.
	 * @param masterDetails contains the IP details of the master node.
	 * @return <i>void</i> - does not return any value
	 */
	public void setMasterDetails(IPDetails masterDetails) {
		this.masterDetails = masterDetails;
		
	}
	/**
	 * <p>
	 * Set the ip details of slave node to the Object of type IPDetails.
	 * @param nodeDetails contains the IP details of the slave node.
	 * @return <i>void</i> - does not return any value
	 */
	public void setNodeDetails(List<IPDetails> nodeDetails) {
		this.nodeDetails = nodeDetails;
		
	}
	/**
	 * <p>
	 * Get the ip details of master node from the Object of type IPDetails.
	 * @param none no parameter required
	 * @return <i>IPDetails</i> - does not return any value
	 */
	public IPDetails getMasterDetails() {
		return masterDetails;
	}
	/**
	 * <p>
	 * Get the ip details of slave node from the Object of type IPDetails.
	 * @param none no parameter required
	 * @return <i>List<IPDetails></i> - does not return any value
	 */
	public List<IPDetails> getNodeDetails() {
		return nodeDetails;
	}
	/**
	 * <p>
	 * Set the number of instances which is slave nodes on aws cluster exluding master.
	 * @param size integer representing the number of instances.
	 * @return <i>void</i> - does not return any value
	 */
	public void setNumOfInstances(int size) {
		this.instanceCount = size;
	}
	/**
	 * <p>
	 * Get the number of instances which is slave nodes on aws cluster exluding master.
	 * @param none no parameter required
	 * @return <i>int</i> - number of salve nodes on aws
	 */
	public int getNumOfInstances() {
		return instanceCount;
	}
	/**
	 * <p>
	 * Get the name of s3 bucket on aws.
	 * @param none no parameter required
	 * @return <i>String</i> - representing the name of s3 bucket on aws.
	 */
	public String gets3Bucket() {
		return s3Bucket;
	}
}
