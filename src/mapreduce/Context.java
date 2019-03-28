package mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * <h1>Context Class</h1>
 * <h3>Maps ouput key/value pairs and Reduces output key/value pairs.</h3>
 *
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Context{

    private Integer jobId;
    private String globalVal;
	public Map<Integer, ObjectOutputStream> mapStream;
    public Map<Integer, BufferedWriter> reducerStream;

    /**
     * <p>
     * Context default constructor
     */
    public Context() {}

    /**
     * <p>
     * Context constructor
     * <p>Initialize Context object with job identifier, global value and creates hash maps for both
     * mapping and reducing keys.
     * @param jobId Integer jobId identifier
     * @param globalVal String value used to store some parameters which can be used across the framework
     */
    public Context(Integer jobId, String globalVal) {
        this.jobId = jobId;
        this.globalVal = globalVal;
        mapStream = new HashMap<>();
        reducerStream = new HashMap<>();
    }

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
     * Writes the mapper output to a context.
     * @param key The key used as mapper output
     * @param value The value of mapper output
     * @throws IOException
     */
    public void writeMapper(Writable key, Writable value) throws IOException {
        int hash = key.hashCode();

        if (mapStream.containsKey(hash)) {
            ObjectOutputStream oStream = mapStream.get(hash);
            oStream.writeObject(value);
            oStream.reset();
        }
        else {
            String fName = "output-"+ String.valueOf(jobId)+"/output/"+String.valueOf(hash)+"/"+String.valueOf(jobId);
            File file = new File(fName);
            file.getParentFile().mkdirs();
            file.createNewFile();
            ObjectOutputStream oStream = new ObjectOutputStream(new FileOutputStream(file));
            oStream.writeObject(key);
            oStream.reset();
            oStream.writeObject(value);
            oStream.reset();
            mapStream.put(hash,oStream);
        }
    }

    /**
     * Writes the reducer output to the output folder.
     * @param key The key used as reducer output
     * @param value The value of reducer output
     * @throws IOException
     */
    public void writeReducer(Writable key, Writable value) throws IOException {
        int hash = key.hashCode();

        if (reducerStream.containsKey(hash)) {
        	BufferedWriter oStream = reducerStream.get(hash);
            oStream.write(key.getString().substring(0,key.getString().length()-1)+","+value.getString());
            oStream.flush();
        }
        else {
            String fName = "output-"+ String.valueOf(jobId)+"/output/"+String.valueOf(hash)+"/"+String.valueOf(jobId);
            File file = new File(fName);
            file.getParentFile().mkdirs();
            file.createNewFile();            
            BufferedWriter oStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
            oStream.write(key.getString().substring(0,key.getString().length()-1)+","+value.getString());
            oStream.flush();
            reducerStream.put(hash,oStream);
        }
    }

    /***
     * This method will close the output streams used to write map output
     */
    public void closeMapStream(){
		for (Map.Entry<Integer, ObjectOutputStream> element : mapStream.entrySet()) {
			try {
				ObjectOutputStream stream =  element.getValue();
				stream.writeObject(null);
				stream.close();
				element.setValue(null);
			}catch (IOException e) {
				e.printStackTrace();
			}
		}
    }

    /***
     * This method will close the output streams used to write reducer output
     */
    public void closeReduceStream(){
		for (Map.Entry<Integer, BufferedWriter> element : reducerStream.entrySet()) {
			try {
				BufferedWriter stream =  element.getValue();
				stream.close();	
				element.setValue(null);
			}catch (IOException e) {
				e.printStackTrace();
			}
		}
    }
}
