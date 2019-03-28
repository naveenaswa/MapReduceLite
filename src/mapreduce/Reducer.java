package mapreduce;

import java.io.IOException;
import java.util.List;

/**
 * <h1>Reducer class</h1>
 * <h3>This is the base Reducer class which the user has to extend</h3>
 * <h3>Reduces a set of intermediate values which share a key to a smaller set of values.</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Reducer {

    /***
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation is an identity function.
     * @param key The key to the reduce function. This is the key from the map output
     * @param values The list of values for the given key.
     * @param context This is the Context object used to write the reducer output
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduce(Writable key, List<Writable> values, Context context) throws IOException, InterruptedException {
        for(Writable value : values) {
            context.writeReducer(key, value);
        }
    }
}
