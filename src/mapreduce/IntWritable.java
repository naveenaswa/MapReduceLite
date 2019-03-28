package mapreduce;

/**
 * <h1>IntWritable class</h1>
 * <h3>A class which represents Integer data type and is serializable</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class IntWritable implements Writable,java.io.Serializable {

	private static final long serialVersionUID = 4338647641044997035L;
	Integer i;

    /***
     * Default constructor
     */
    public IntWritable() {}

    /***
     * Cosntructor which creates a IntWritable object from the given Integer object
     * @param i The double value
     */
    public IntWritable(Integer i) {
        this.i = i;
    }

    /***
     * Method used to return the Integer value of the object.
     * @return The integer value
     */
    public Integer getInteger() {
        return this.i;
    }

    /***
     * Method used to set the Integer value of the IntWritable object.
     * @param i The integer value to be set
     */
    public void setInteger(Integer i) {
		this.i = i;
	}

    /***
     * Method used to check quality between IntWritable objects.
     * @param o The other object to compare to
     * @return true or false based on the equality.
     */
	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntWritable that = (IntWritable) o;

        return !(i != null ? !i.equals(that.i) : that.i != null);

    }

    /***
     * Method used to return the string of the IntWritable object
     * @return string of the IntWritable object
     */
    @Override
    public String getString() {
        return i.toString()+"\n";
    }

    /***
     * Returns the hash code of the Integer value.
     * @return Hash code of the integer value.
     */
    @Override
    public int hashCode() {
        return i != null ? i.hashCode() : 0;
    }
}
