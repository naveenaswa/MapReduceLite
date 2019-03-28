package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * <h1>Slave class</h1>
 * <h3>The Slave nodes listen to and act upon the commands from the master node. Each slave node has a
 * ServerSocket listening to messages from the master. Upon arrival of a connection, a separate thread is spawned to
 * handle the request.</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Slave implements Runnable {
	private static ServerSocket serverSocket = null;
	private Socket conn = null;
	private static Configuration slaveConf;	
	private static IPDetails myIpDetails;
	private static String myIpString;

    /**
     * The constructor used to initialize the socket connection
     * @param conn The socket
     * @throws IOException
     */
	public Slave(Socket conn) throws IOException {
		System.out.println("invoking the constructor server side and returning a runnable");
		this.conn = conn;
	}

    /**
     * This is main method of the slave.
     * @param conf The configuration object to be used by the slave.
     */
	public static void runSlave(Configuration conf) throws IOException {
		setConf(conf);
		myIpString = getIP();
		getMyIpDetails();
		slave();
	}

    /**
     * Sets the slave configuration
     * @param conf The configuration to be set to the slave.
     */
	private static void setConf(Configuration conf){
		slaveConf = conf;
	}

    /**
     * Computes the IP address of the current slave node.
     */
	private static void getMyIpDetails() {

		for(IPDetails details : slaveConf.getNodeDetails()) {
			if(details.getIpAddr().equals(myIpString)){
				myIpDetails = new IPDetails(details.getIpAddr(), details.getPort());
				return;
			}
		}
	}

    /**
     * This method listens to master's messages and spawns a thread to handle each message.
     */
	public static void slave()
	{
		Socket socket = null;
		try {
			serverSocket = new ServerSocket(Integer.parseInt(myIpDetails.getPort()));
			while (true) {
				System.out.println("In slave: waiting for a request from master");
				socket = serverSocket.accept();
				Runnable runnable = new Slave(socket);
				Thread thread = new Thread(runnable);
				System.out.println("In slave: executing the runnable");
				thread.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}                  
	}

    /**
     * This method receives messages from master and handles the Jobs accordingly.
     */
	public synchronized void run()
	{
		try
		{
			System.out.println("Runnable in slave: Just connected to "
					+ conn.getRemoteSocketAddress());
			ObjectInputStream inToServer = new ObjectInputStream(conn.getInputStream());

			String header = inToServer.readUTF();
			System.out.println(header);
			if(header.equals("map")){
				System.out.println("Entered Mapping stage");
				Job job =  (Job) inToServer.readObject();                           
				mapJob(job);
				ProcessBuilder aws = new ProcessBuilder().inheritIO().command("aws", "s3", "cp", "output-"+String.valueOf(job.jobId)+"/output", slaveConf.gets3Bucket()+"shuffle", "--recursive");
				Process p = aws.start();
				try{
					p.waitFor();
				}catch(Exception e){
				}
				Socket socket = new Socket(slaveConf.getMasterDetails().getIpAddr(),Integer.parseInt(slaveConf.getMasterDetails().getPort()));
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());					
				out.writeUTF("shuffled");
				out.writeObject(new String("Shuffle dummy"));
				socket.close();
			}
			else if(header.equals("reduce")){
				JobReduce jobReduce = (JobReduce)inToServer.readObject();
				System.out.println("Entered Reducing stage");
				reduceJob(jobReduce);
				for(String key : jobReduce.keys) {
					//ProcessBuilder aws = new ProcessBuilder().inheritIO().command("aws", "s3", "cp", "output-"+String.valueOf(jobReduce.jobId)+"/output/"+key, slaveConf.getOutputDirPath(), "--recursive");
					ProcessBuilder aws = new ProcessBuilder().inheritIO().command("aws", "s3", "cp", "output-"+String.valueOf(jobReduce.jobId)+"/output/"+key+"/"+jobReduce.jobId, slaveConf.getOutputDirPath()+key);
					Process p = aws.start();
					try{
						p.waitFor();
					}catch(Exception e){
					}
				}
				Socket socket = new Socket(slaveConf.getMasterDetails().getIpAddr(),Integer.parseInt(slaveConf.getMasterDetails().getPort()));
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeUTF("reduced");
				out.writeObject(new String("Reduce dummy"));
				socket.close();                   
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}

    /**
     * This method services the reduce job sent by the master. It does the reduce stage and writes to S3 output bucket.
     * @param jobReduce The reduce job to be serviced.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
	public static void reduceJob(JobReduce jobReduce) throws IOException, ClassNotFoundException, InterruptedException, InstantiationException, IllegalAccessException
	{
		System.out.println("ReduceJob:");
		System.out.println("Job:" + jobReduce);
		for(String key : jobReduce.keys) {
			AmazonS3 s3 = new AmazonS3Client();
			Region usEast1 = Region.getRegion(Regions.US_EAST_1);
			s3.setRegion(usEast1);
			AmazonS3URI URL1 = new AmazonS3URI(slaveConf.gets3Bucket()+"shuffle/"+key);			
			String bucketName = URL1.getBucket();
			String path = URL1.getURI().getPath().substring(1)+"/";
			ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
			.withBucketName(bucketName)
			.withPrefix(path)
			.withDelimiter("/"));
			List<String> files = new ArrayList<>();
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {			
				if(!files.contains(objectSummary.getKey()) && objectSummary.getSize() > 0) {
					files.add(objectSummary.getKey());
				}   
			}
			System.out.println("Files list:"+files);
			List<Writable> values = new ArrayList<>();
			Writable keyR = null;
			for(String file : files){
				System.out.println("File: "+file);
				S3Object object = s3.getObject(
						new GetObjectRequest(URL1.getBucket(), file));
				InputStream objectData = object.getObjectContent();				
				ObjectInputStream oStream = new ObjectInputStream(objectData);
				keyR = DataTypeFactory.getObj(oStream.readObject());
				Writable obj = null;
				while ((obj = DataTypeFactory.getObj(oStream.readObject())) != null)
					values.add(obj);
				object.close();
			}
			Reducer reducer = slaveConf.getReducerClass().newInstance();
			Context context = new Context(jobReduce.jobId, slaveConf.getGlobalVal());
			reducer.reduce(keyR, values, context);			
			context.closeReduceStream();
		}
	}

    /**
     * This method services the map job sent by the master. The intermediate mapper output is written to S3 bucket.
     * @param job The map job to be serviced.
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
	public static void mapJob(Job job) throws IllegalAccessException, InstantiationException{

		Context writeContext = new Context(job.jobId, slaveConf.getGlobalVal());
		AmazonS3URI URL1 = new AmazonS3URI(slaveConf.getInputDirPath());			
		AmazonS3 s3Client = new AmazonS3Client();
		IntWritable mapInputKey = new IntWritable();
		Text mapInputValue = new Text();
		Mapper map = slaveConf.getMapperClass().newInstance();
		for ( String file : job.fileList){
			int index=0;
			try{
				S3Object object = s3Client.getObject(
						new GetObjectRequest(URL1.getBucket(), file));
				InputStream objectData = object.getObjectContent();
				BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(objectData)));
				String nline;
				while ((nline = br.readLine()) != null){
					mapInputKey.setInteger(index);
					mapInputValue.setText(nline);
					map.map(mapInputKey, mapInputValue, writeContext);
					index++;
				}
				objectData.close();

			}catch(Exception e){
				e.printStackTrace();
			}
		}
		writeContext.closeMapStream();
	}

    /**
     * Computes the IP address of the current slave node.
     * @return Returns the IP address of the slave node.
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
}
