package mapreduce;

import java.io.IOException;

/**
 * <h1>Mapper class</h1>
 * <h3>This is the base Mapper class which the user has to extend</h3>
 * <h3>Maps input key/value pairs to a set of intermediate key/value pairs.</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Mapper{

    /***
     * Called once for each key/value pair in the input split.
     * Most applications should override this, but the default is the identity function.
     * @param key The key to the map function. This is the offset from start of a input file
     * @param value The value is the content of a line from a input file
     * @param context This is the Context object used to write the mapper output
     * @throws IOException
     * @throws InterruptedException
     */
    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
        context.writeMapper(key, value);
    }
}
