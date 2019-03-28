package mapreduce;

import java.net.*;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * <h1>Master class</h1>
 * <h3>The master node creates and distributes map and reduce jobs among the different slave nodes.</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Master implements Runnable{ 

	private static ServerSocket serverSocket = null;
	private Socket conn = null;	
	private static final int NUMBER_OF_CORES = 4;
	private static AtomicInteger instancesCompletedShuffling = new AtomicInteger(0);
	private static AtomicInteger instancesCompletedSorting = new AtomicInteger(0);
	private static Map<Integer, List<String>> instanceFilesMap = new HashMap<>();
	private static Integer mapJobsCount = 0;
	private static Integer reduceJobsCount = 0;
	private static Configuration masterConf = null;

	/**
	 * The constructor used to initialize the socket connection
	 * @param conn The socket
	 * @throws IOException
     */
	public Master(Socket conn) throws IOException
	{
		this.conn = conn;
	}

	/**
	 * This is main method of the master.
	 * @param conf The configuration object to be used by the master.
     */
	public static void runMaster(Configuration conf) {
		masterConf = conf;    	
		try {
			distributeInputFiles();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    /**
     * Distributes the input files among the slaves
     * @throws UnknownHostException
     * @throws IOException
     */
	public static void distributeInputFiles() throws UnknownHostException, IOException {

		distributeFilesToNodes();

		createMapJobs();

		listen();
	}

    /**
     * This method handles all messages received by the master from all the slave nodes
     */
	public synchronized void run()
	{
		try
		{
			ObjectInputStream in = new ObjectInputStream(conn.getInputStream());
			String header = in.readUTF();
            // This message is sent by a slave after it has finished the shuffle stage.
			if (header.equals("shuffled")) {
				System.out.println("Ended shuffle on Master");
				instancesCompletedShuffling.incrementAndGet();
				// num of instances * num of cores
				if (instancesCompletedShuffling.get() == mapJobsCount) {
					sendReduceMessage();
				}
				System.out.println("End shuffle");
			}
            // This message is sent by a slave after it has finished the reduce stage.
			else if (header.equals("reduced")) {
				System.out.println("Ended reduce on Master");
				instancesCompletedSorting.incrementAndGet();
				// number of instances * num of cores
				if (instancesCompletedSorting.get() == reduceJobsCount) {
					System.out.println("The Map Reduce Job is complete !!!");
                    System.out.println("The cluster can now be terminated.");
                }
				System.out.println("End sort");                
			}

			conn.close();
		}catch(SocketTimeoutException s)
		{
			System.out.println("Socket timed out!");
		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}

    /**
     * This method sends a reduce message to the slaves
     * @throws UnknownHostException
     * @throws IOException
     */
	private void sendReduceMessage() throws UnknownHostException, IOException {
		
		Map<Integer, List<String>> keyToNodeMap = distributeKeysToNodes();
		
		createReduceJobs(keyToNodeMap);
	}

    /**
     * This method distributes the input files to the slaves
     */
	public static void distributeFilesToNodes() {
		Map<String, Long> sortedFileMap = getFilesFromS3(masterConf.getInputDirPath());
		int count = 0;

		for (String fileName: sortedFileMap.keySet()) {
			int currentInstance = count % masterConf.getNodeDetails().size();
			List<String> existingFileNames = new ArrayList<>();
			if (instanceFilesMap.containsKey(currentInstance)) {
				existingFileNames = instanceFilesMap.get(currentInstance);
				existingFileNames.add(fileName);
			} else {
				existingFileNames.add(fileName);
			}
			instanceFilesMap.put(currentInstance, existingFileNames);
			count ++;
		}
	}

    /**
     * This method creates map jobs and sends them to the slave nodes
     * @throws NumberFormatException
     * @throws UnknownHostException
     * @throws IOException
     */
	public static void createMapJobs() throws NumberFormatException, UnknownHostException, IOException {
		int nodeNumber=0;

		// Iterating over each node
		for (IPDetails node : masterConf.getNodeDetails()) {
			Map<Integer, List<String>> coreFilesMap = new HashMap<Integer, List<String>>();

			int iter = 0;
			if (instanceFilesMap.containsKey(nodeNumber)) {
				for (String file: instanceFilesMap.get(nodeNumber)) {
					int currentCoreIndex = iter % NUMBER_OF_CORES;
					List<String> existingFileNames = new ArrayList<String>();
					if (coreFilesMap.containsKey(currentCoreIndex)) {
						existingFileNames = coreFilesMap.get(currentCoreIndex);
					}
					existingFileNames.add(file);
					coreFilesMap.put(currentCoreIndex,existingFileNames);
					iter++;
				}

				for (Integer coreNum : coreFilesMap.keySet()) {
					Job job = new Job(coreFilesMap.get(coreNum));
					Socket client = new Socket(node.getIpAddr(), Integer.parseInt(node.getPort()));//6072
					ObjectOutputStream outToServer = new ObjectOutputStream(client.getOutputStream());
					outToServer.writeUTF("map");
					outToServer.writeObject(job);
					client.close();
					mapJobsCount++;
				} 
			}
			nodeNumber++;
		}
		System.out.println("map job count : " + mapJobsCount);
	}

    /**
     * The master listens to messages from the slaves and spawns a new thread each time it receives a message.
     * @throws NumberFormatException
     * @throws IOException
     */
	public static void listen() throws NumberFormatException, IOException{

		Socket socket = null;
		serverSocket = new ServerSocket(Integer.parseInt(masterConf.getMasterDetails().getPort()));
		while (true) {
			socket = serverSocket.accept();
			Runnable runnable = new Master(socket);
			Thread thread = new Thread(runnable);
			thread.start();
		}

	}

    /**
     * This method is used to distribute keys to the slave nodes.
     * @return The keys to be sent for each node.
     */
	public static Map<Integer, List<String>> distributeKeysToNodes() {
		List<String> keyNames = getFoldersFromS3(masterConf.gets3Bucket()+"shuffle");
		Map<Integer, List<String>> keyToNodeMap = new HashMap<>();
		int iter = 0;
		for (String key: keyNames) {
			int currentNodeIndex = iter % masterConf.getNodeDetails().size();
			List<String> existingKeys = new ArrayList<String>();

			if (keyToNodeMap .containsKey(currentNodeIndex)) {
				existingKeys = keyToNodeMap.get(currentNodeIndex);
			}
			existingKeys.add(key);
			keyToNodeMap.put(currentNodeIndex,existingKeys);
			iter++;
		}
		return keyToNodeMap;
	}

    /**
     * This method creates reduce jobs and sends them to the slave nodes
     * @param keyToNodeMap The keys to be sent for each node in a hash map.
     * @throws NumberFormatException
     * @throws UnknownHostException
     * @throws IOException
     */
	private void createReduceJobs(Map<Integer, List<String>> keyToNodeMap) throws NumberFormatException, UnknownHostException, IOException {
		int nodeNumber = 0;
		for (IPDetails node: masterConf.getNodeDetails()) {
			Map<Integer, List<String>> coreKeysMap = new HashMap<>();

			int keyIter = 0;
			if(keyToNodeMap.containsKey(nodeNumber)) {
				for (String key: keyToNodeMap.get(nodeNumber)) {
					int currentCoreIndex = keyIter % NUMBER_OF_CORES;
					List<String> existingKeys = new ArrayList<>();              
					if (coreKeysMap.containsKey(currentCoreIndex)) {
						existingKeys = coreKeysMap.get(currentCoreIndex);
					}
					existingKeys.add(key);
					coreKeysMap.put(currentCoreIndex,existingKeys);
					keyIter++;
				}
				for (List<String> keys : coreKeysMap.values()) {
					JobReduce jobReduce = new JobReduce(keys);
					Socket client = new Socket(node.getIpAddr(), Integer.parseInt(node.getPort()));//6072
					ObjectOutputStream outToServer = new ObjectOutputStream(client.getOutputStream());
					outToServer.writeUTF("reduce");
					outToServer.writeObject(jobReduce);
					client.close();
					reduceJobsCount++;
				}
			}
			nodeNumber++;            
		}
	}

    /**
     * This method takes a folder path on a S3 bucket as argument and returns all the files along with their sizes.
     * @param URI The folder path on S3
     * @return List of files in the given S3 folder along with the file sizes.
     */
	public static Map<String,Long> getFilesFromS3(String URI){
		AmazonS3 s3 = new AmazonS3Client();
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usEast1);
		AmazonS3URI URL1 = new AmazonS3URI(URI);
		String bucketName = URL1.getBucket();
		String path = URL1.getURI().getPath().substring(1)+"/";
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
		.withBucketName(bucketName)
		.withPrefix(path)
		.withDelimiter("/"));
		Map<String,Long> files = new HashMap<>();
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			long sizeInKb =  (objectSummary.getSize()/(1024));
			if(!files.containsKey(objectSummary.getKey()) && sizeInKb > 0) {
				files.put(objectSummary.getKey(), sizeInKb);
			}    
		}
		System.out.println("input files");
		for (String file: files.keySet()) {
			System.out.println(file);
		}
		Map<String, Long> sortedMapAsc = sortByComparator(files, true);
		return sortedMapAsc;
	}

    /**
     * This method takes a S3 path as argument and returns all the folders in the given path.
     * @param URI A S3 path
     * @return List of folders in the given path
     */
	public static List<String> getFoldersFromS3(String URI){
		AmazonS3 s3 = new AmazonS3Client();
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usEast1);
		AmazonS3URI URL1 = new AmazonS3URI(URI);
		String bucketName = URL1.getBucket();
		String path = URL1.getURI().getPath().substring(1)+"/";
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
		.withBucketName(bucketName)
		.withPrefix(path)
		.withDelimiter("/"));
		List<String> keyFolders = new ArrayList<>();
		int i = 1;
		for (String folder : objectListing.getCommonPrefixes()) {         	
			if(i!=1){
				String[] f =folder.split("/");            	
				keyFolders.add(f[1]);
			}
			i++;
		}
		return keyFolders;
	}

    /**
     * This methods sorts a given map by value.
     * @param unsortMap The map to be sorted.
     * @param order The order in which the map should be sorted. True implies ascending sort.
     * @return
     */
	private static Map<String, Long> sortByComparator(Map<String, Long> unsortMap, final boolean order)
	{
		List<Entry<String, Long>> list = new LinkedList<Entry<String, Long>>(unsortMap.entrySet());
		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Long>>()
				{
			public int compare(Entry<String, Long> o1,
					Entry<String, Long> o2)
			{
				if (order)
				{
					return o1.getValue().compareTo(o2.getValue());
				}
				else
				{
					return o2.getValue().compareTo(o1.getValue());

				}
			}
				});

		// Maintaining insertion order with the help of LinkedList
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		for (Entry<String, Long> entry : list)
		{
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

}
