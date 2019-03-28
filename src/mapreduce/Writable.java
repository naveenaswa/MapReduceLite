package mapreduce;
/**
 * <h1>Writable Interface</h1>
 * <h3>This interface is used to list out all the mandatory methods to be
 * implemented by any Data type class to be used in the Map Reduce framework
 * </h3>
 *
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public interface Writable{ 
	
	public abstract String getString();
	
	public abstract int hashCode();
	
    public abstract boolean equals(Object o);
}
