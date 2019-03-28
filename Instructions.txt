1. Requirement
	bash    >=4.0
	openjdk	>=1.7
	awscli 	>=1.10
2. How to use the mapreduce framework
	2.1 Overview
		Our mapreduce framework and dependency are included in a zip file. This zip file has following files and folders
		1.mapreduce.jar		main code of the framework
		2.lib/			all the dependency required by the framework
		3.bin/			script to launch cluster, prepare running environment and run job.
		4.example/		test code from previous assignment
			A3
			A4
			A5	
		In order to mapreduce framework, please add this mapreduce.jar and all dependency library in lib folder to your project.
	2.2 MrJob class
		The mrjob class is similiar to the Job class in the hadopp mapreduce framework the class which you can set up the running parameters and lauch the the job. 
		Following is the an example program show how to use the MrJob class:
		import mapreduce.MrJob;
		public class TestMain {
		    public static void main(String[] args) throws Exception {

				MrJob mrjob = new MrJob();
				mrjob.setInputDirPath("s3://hua9/a5");
				mrjob.setOutputDirPath("s3://hua9/output/");
				mrjob.setMapperClass(AirlineMapper.class);
				mrjob.setReducerClass(AirlineReducer.class);
				mrjob.setMapperOutputKey(Text.class);
				mrjob.setMapperOutputValue(Text.class);
				mrjob.setReducerOutputKey(Text.class);
				mrjob.setReducerOutputValue(Text.class);
				try {
					mrjob.run();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		The main class's name is TestMain, this is the default name used in the script to start a the job the cluster, so if you use the TestMain as your main class's name, you don't need to modify name in the script.
		First we need create a new instance of MrJob, in this class you need to provide following information:
		Mandatory:
		InputDirPath		Description: This is the folder which include all the compressed raw data.
					Requirement: This path should be a s3 path. All the files in the folder should be gzip compressed text files.
					Format: "s3://[bucketname]/folder1/[..]/folder2". No slash on the end of the path.
		OutputDirPath		Description: This is the folder which has all the output files.
					Requirement: This path should be a s3 path.
					Format: "s3://[bucketname]/folder1/[..]/folder2/". The path should end with "/".
		MapperClass		Description: This is the file name of the user mapper class
					Requirement: See detail on the Mapper section
					Format: classname.class
		ReducerClass		Description: This is the file name of the user reducer class
					Requirement: See detail on the Reducer section
					Format: classname.class
		MapperOutputKey	Description: This is the type of the key from the user's mapper
					Requirement: Pick up one of those four option [Text.class, DoubleWritable.class, FloatWritable.class, IntWritable.class]
					Format: type.class
		MapperOutputValue	Description: This is the type of the value from the user's mapper
					Requirement: Pick up one of those four option [Text.class, DoubleWritable.class, FloatWritable.class, IntWritable.class]
					Format: type.class
		ReducerOutputKey	Description: This is the type of the key from the user's reducer
					Requirement: Pick up one of those four option [Text.class, DoubleWritable.class, FloatWritable.class, IntWritable.class]
					Format: type.class
		ReducerOutputValue	Description: This is the type of the value from the user's reducer
					Requirement: Pick up one of those four option [Text.class, DoubleWritable.class, FloatWritable.class, IntWritable.class]
					Format: type.class
		Optional:
		GlobalVal		Description: This is a global varible which every mapper and reducer can know which is similar to the configuration in mapreduce.
					Requirement: Any string that find fit into a Java String type. 
					Format: string
		After given all the parameter, you can call run() to start the job.
	2.3 Mapper
		You can customize the mapper logic by implementing map function in a class extend the mapper in the framework. Following is the a example of user mapper class.
		import mapreduce.*;
		public class AirlinePriceMapper extends Mapper{
			    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
			    	AirlineParser FParser = new AirlineParser();
			    	if (!FParser.map(value.getString())){
			    		return;
			    	}
			    	context.writeMapper(new Text(FParser.Carrier), new DoubleWritable(FParser.Price));
			}
		}
		Mapreduce framework will decompress all the files in the input folder and pass every line to mapper function and expect key value pair as the output from the mapper function. The input of the mapper function are a key value pair and a context instance you can use to write the output into. The key contains line number of current value for each input file and value store all the charactors in this line. You can't change the input of mapper. You can use the getString in this Writable to get string from value. In this example, we use value.getString() to get String and feed it into parser.
		You may want to give a key value pair to reducer. In this example, it give a Text type key and a DoubleWritable type value. 
	2.4 Reducer
		Once the mapping is done. The mapreduce will give each reduce function a key and list of value which share the same key. The type of both key and value is Writable, this is the abstract type. So you need to cast the abstract type to real type according to the type you use in mapper. The real type can be one of the four types-Text.class, DoubleWritable.class, FloatWritable.class, IntWritable.class. Following is a customized reducer example:
		import mapreduce.*;
		public class CheapAirlineReducer extends Reducer {
		    @Override
		    protected void reduce(Writable key, List<Writable> values, Context context) throws IOException, InterruptedException {

			SimpleRegression regression = new SimpleRegression();

			for(Writable v: values){

				Text value = (Text) v;
			    String[] valueArr = value.getString().split("\\|");

			    String elapsedTimeString = valueArr[0];
			    String priceString = valueArr[1];

			    Double elpasedTime = Double.parseDouble(elapsedTimeString);
			    Double price = Double.parseDouble(priceString);

			    // Adding each data pair to the linear regression computation
			    regression.addData(elpasedTime, price);
			}

			// Key remains unchanged, value is the intercept and slope obtained from the linear regression
			context.writeReducer((Text)key, new Text(String.valueOf(regression.getIntercept())+","+String.valueOf(regression.getSlope())));

		    }
		}
	In this example, the for loop iterates all the Writable variable. In the loop, we cast the Writable to Text which has the elapsed time and price of a flight. 
	2.5 Run the program
		2.5.0 Preparation
			Decompress the zip file and go to this folder in terminal, I will call this folder root, I will assume you are under this folder after this line.
		2.5.1 Dependency
			Copy all the dependent library into the libs folder under root.
		2.5.2 Source Path
			Change userPath in bin/cluster.sh to /fulltosource/*.java, then the cluster.sh will compile your code automatically.
		2.5.3 Cluster Preparation
			Change the value of following parameter
				Varible typeOfEC2 is the instance type you want to launch on aws,  for example typeOfEC2="m3.xlarge" 
				Varible keyName is name of the public you want to use on the aws instance, for example keyName="MAPR"
				Varible keyPath is the full path to the file store the private key which you can login in to the aws instance, keyPath="/home/radioer/CS6240/MAPR.pem"
				Varible yourBucketName is the name of the bucket you want store and save the shuffled date and output, this bucket should be the same bucket you use in the OutputDirPath, for example, yourBucketName="hua9"
			Go to bin/ under root, run ./cluster.sh c #_of_nodes, it will launch a cluster of 10 computing node and controller node on aws.
			When aws is preparing the cluster, the script checks the status of cluster every 10 seconds and print the status on screen, it will quit and return to terminal as long as the cluster is ready.
		2.5.4 Job Running
			Go to bin/ under root, if you are not using default class TestMain, then change the run TestMain on line 105 and 102 to the name of main class, if you use package please use package.classname format. If you want to pass arguments to your program, write your arguments after the name of the main class on line 105 and 102, please use space as the delimitor. Then run ./cluster.sh s #_of_nodes, it will compile your code with the mapreduce framework, upload the all jars to the cluster and start the job. 
		2.5.5 Job Cancellation
			You can kill the job at any time you want. Just go to bin/, run the command "./cluster.sh k #_of_nodes". This command delete all the mapreduce framework and user code on cluster and kill all the running process.
		2.5.6 Cluster Termination
			Just go to bin/, run the command "./cluster.sh e #_of_nodes". This will terminate the cluster and save money for you.
		2.5.7 Troubleshoot
			Because of possible heavy network traffic and DDOS protection, the file access on AWS may be blocked, you can run the command under bin/ "./cluster.sh ip" to check the status of the computing node. 
	2.7 Output Format
		Each key from the output of reduce will create a file in output folder and each file contains multi line of key value pair.
		Example: B6,495.4575267612215
3.How to run test example
	3.0 Preparation
		Change the value of following parameter
		Varible typeOfEC2 is the instance type you want to launch on aws,  for example typeOfEC2="m3.xlarge" 
		Varible keyName is name of the public you want to use on the aws instance, for example keyName="MAPR"
		Varible keyPath is the full path to the file store the private key which you can login in to the aws instance, keyPath="/home/radioer/CS6240/MAPR.pem"
		Varible yourBucketName is the name of the bucket you want store and save the shuffled date and output, this bucket should be the same bucket you use in the OutputDirPath, for example, yourBucketName="hua9t"
	3.1 A3 Find median and mean
		3.1.1 Change cluster.sh line 8 userpath=../example/A3/src/*.java
		3.2.2 Change the output folder on the line 16 in example/A3/src/TestMain.java from "s3://hua9/output/" to "s3://(yourbucketname)/output/"
		3.2.3 Run the command in bin/ folder "./cluster.sh s 10"
		3.3.4 Once you see the job is complete, you can see the result on AWS, each file contains the mean and median price, each carrier has a file in output folder.
		3.3.5 Run the command in bin/ folder "./cluster.sh k 10"
	3.2 A4 Find the cheapest airline and get the median price for each week
		3.2.1 Change cluster.sh line 8 userpath=../example/A4/src/*.java
		3.2.2 Change the output folder on the line 11 in example/A4/src/TestMain.java from "s3://hua9/output/" to "s3://(yourbucketname)/output/"
		3.2.3 Run the command in bin/ folder "./cluster.sh s 10"
		3.2.4 You will get the scope and interception of each carrier for every year, then run the command in bin/ folder "./runA4.sh N", you will get which carrier for N.
		3.2.5 Run the command in bin/ folder "./cluster.sh k 10" to kill previous job
		3.2.6 Change cluster.sh line 8 userpath=../example/A4/src/LeastMedian/*.java to build the MR job get median price for every week.
		3.2.7 Change pass cheapest carrier as a argument by following steps
			On line 105 in cluster.sh, change 
			"cd mapreduce; PATH=$PATH:/sbin; nohup java -cp ./*:./libs/*:./libs/third-party/lib/* TestMain >out 2>err </dev/null" to
			"cd mapreduce; PATH=$PATH:/sbin; nohup java -cp ./*:./libs/*:./libs/third-party/lib/* LeastMedian.TestMain (CheapestCarrier) >out 2>err </dev/null"
			On line 109 in cluster.sh, change 
			"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/CS6240/MAPR.pem ec2-user@$masterIPP "rm -Rf mapreduce; aws s3 cp s3://${yourBucketName}/mapreduce ./mapreduce --recursive; cd mapreduce; PATH=$PATH:/sbin;java -cp ./*:./libs/*:./libs/third-party/lib/* TestMain" to
			"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/CS6240/MAPR.pem ec2-user@$masterIPP "rm -Rf mapreduce; aws s3 cp s3://${yourBucketName}/mapreduce ./mapreduce --recursive; cd mapreduce; PATH=$PATH:/sbin;java -cp ./*:./libs/*:./libs/third-party/lib/* LeastMedian.TestMain TestMain (CheapestCarrier)"
		3.2.8 Run the command in bin/ folder "./cluster.sh s 10"
		3.3.9 Once you see the job is complete, you can see the result on AWS
		3.3.10 Run the command in bin/ folder "./cluster.sh k 10"
	3.3 A5 Find all the connections
		3.3.1 Change cluster.sh line 8 userpath=../example/A5/src/*.java
		3.3.2 Change the output folder on the line 16 in example/A5/src/TestMain.java from "s3://hua9/output/" to "s3://(yourbucketname)/output/"
		3.3.3 Undo the 3.2.7
		3.3.4 Run the command in bin/ folder "./cluster.sh s 10"
		3.3.5 Once you see the job is complete, run the command "./runA5.sh"
		3.3.6 Check the ./a5User/results.csv
		3.3.7 Run the command in bin/ folder "./cluster.sh k 10"
