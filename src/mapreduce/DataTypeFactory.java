package mapreduce;

/**
 * <h1>DataTypeFactory class</h1>
 * <h3>Use to return the relevant object which implements the Writable interface</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public final class DataTypeFactory {

    /***
     *  The default constructor
     */
	private DataTypeFactory() {}

    /***
     * This method typecasts a given object to an instance of the relevant class that implements Writable interface
     * @param obj The object to be typecasted
     * @return the object after being typecasted to a class which implements the Writable interface.
     */
	public static Writable getObj(Object obj) {
		
		if (obj instanceof Text) {
			return (Text)obj;
		}
		if (obj instanceof IntWritable) {
			return (IntWritable)obj;
		}
		if (obj instanceof FloatWritable) {
			return (FloatWritable)obj;
		}
		if (obj instanceof DoubleWritable) {
			return (DoubleWritable)obj;
		}
		
		return null;
	}

    /***
     * This method creates a object of Writable type from the given string object.
     * @param key The key string which has to be typecasted
     * @param className The string which represents the class name
     * @return The new object after being typecasted
     */
	public static Writable getWritable(String key, String className) {
		if (className.equalsIgnoreCase("mapreduce.Text")) {
			return  new Text(key);
		}
		if (className.equalsIgnoreCase("mapreduce.IntWritable")) {
			return  new IntWritable(Integer.parseInt(key));
		}
		if (className.equalsIgnoreCase("mapreduce.DoubleWritabe")) {
			return  new DoubleWritable(Double.parseDouble(key));
		}
		if (className.equalsIgnoreCase("mapreduce.FloatWritable")) {
			return  new FloatWritable(Float.parseFloat(key));
		}
		return null;
	}
}
