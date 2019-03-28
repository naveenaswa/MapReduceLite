package mapreduce;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * <h1>MrJob class</h1>
 * <h3>MrJob is the primary interface for a user to describe a MapReduce job to our framework for execution.
 * The framework tries to faithfully execute the job as described by MrJob.
 * <h3>Normally the user creates the application, describes various facets of the job via Job and then submits
 * the job and monitor its progress.</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class MrJob {
        
    private static Configuration conf = new Configuration();

    /***
     * Sets the input directory path to the MrJob.
     * @param inputDirPath The input directory
     */
    public void setInputDirPath(String inputDirPath) {
        conf.setInputDirPath(inputDirPath);
    }

    /***
     * Sets the output directory path to the MrJob.
     * @param outputDirPath The output directory
     */
    public void setOutputDirPath(String outputDirPath) {
        conf.setOutputDirPath(outputDirPath);
    }

    /***
     * Sets the mapper class of the MrJob.
     * @param mapperClass The mapper class of the MrJob
     */
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        conf.setMapperClass(mapperClass);
    }

    /***
     * Sets the reducer class of the MrJob.
     * @param reducerClass The reducer class of the MrJob
     */
    public void setReducerClass(Class<? extends Reducer> reducerClass) {
        conf.setReducerClass(reducerClass);
    }

    /***
     * Sets the mapper output key of the MrJob.
     * @param mapOutputKey The output key of the mapper
     */
    public void setMapperOutputKey(Class<?> mapOutputKey) {
        conf.setMapOutputKey(mapOutputKey);
    }

    /***
     * Sets the mapper output value of the MrJob.
     * @param mapOutputValue The output value of the mapper
     */
    public void setMapperOutputValue(Class<?> mapOutputValue) {
        conf.setMapOutputValue(mapOutputValue);
    }

    /***
     * Sets the reducer output key of the MrJob.
     * @param reduceOutputKey The output key of the reducer
     */
    public void setReducerOutputKey(Class<?> reduceOutputKey) {
        conf.setReduceOutputKey(reduceOutputKey);
    }

    /***
     * Sets the reducer output value of the MrJob.
     * @param reduceOutputValue The output value of the reducer
     */
    public void setReducerOutputValue(Class<?> reduceOutputValue) {
        conf.setReduceOutputValue(reduceOutputValue);
    }

    /***
     * This is used to store a global variable accessible to all the nodes.
     * @param globalVal The global variable
     */
    public void setGlobalVal(String globalVal) {
        conf.setGlobalVal(globalVal);
    }

    /***
     * Returns the IP address of the node under execution.
     * @return IP address of the current node
     * @throws IOException
     */
    private static String getIP() throws IOException {
        ProcessBuilder aws = new ProcessBuilder().command("/bin/bash", "-c", "ifconfig eth0 | grep \"inet \" | cut -d ':' -f2 | cut -d ' ' -f1");
        String IP="";
        try {
            Process p = aws.start();
            p.waitFor();
            BufferedReader reader = new BufferedReader (new InputStreamReader(p.getInputStream()));
            IP = reader.readLine();
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return IP;
    }

    /***
     * Runs either the master code or the slave code based on the IP.
     * @throws IOException
     */
    public void run() throws IOException {
    	
        readIpDetails();

        if (isMaster()) {
            Master.runMaster(conf);
        } else {
            Slave.runSlave(conf);
        }
    }

    /***
     * Checks if the current node is the master node
     * @return true or false
     * @throws IOException
     */
    private static boolean isMaster() throws IOException {
        String myIp = getIP();
        return myIp.equals(conf.getMasterDetails().getIpAddr());
    }

    /***
     * Reads the IP details file and sets the master node, slave nodes and the number of instances.
     * @throws IOException
     */
    private static void readIpDetails() throws IOException {
    	IPDetails masterDetails;
        List<IPDetails> nodeDetails = new ArrayList<>();
        
        BufferedReader br = new BufferedReader(new FileReader("IP_Details.txt"));
        try {

            String line = br.readLine();
            String[] details = line.split(",");
            String ipAddr = details[0];
            String port = details[1];
            masterDetails = new IPDetails(ipAddr, port);

            while ((line= br.readLine()) != null) {
                details = line.split(",");
                ipAddr = details[0];
                port = details[1];
                IPDetails ipDetails = new IPDetails(ipAddr, port);
                nodeDetails.add(ipDetails);
            }
            conf.setMasterDetails(masterDetails);
        	conf.setNodeDetails(nodeDetails);
        	conf.setNumOfInstances(nodeDetails.size());
        	
        } finally {        	
            br.close();
        }
    }
}
