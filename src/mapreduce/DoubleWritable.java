package mapreduce;

/**
 * <h1>DoubleWritable class</h1>
 * <h3>A class which represents Double data type and is serializable</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class DoubleWritable implements Writable, java.io.Serializable {
	private static final long serialVersionUID = 5824261960594005736L;
	Double d;

    /***
     * Default constructor
     */
    public DoubleWritable() {}

    /***
     * Cosntructor which creates a DoubleWritable object from the given Double object
     * @param d The double value
     */
    public DoubleWritable(Double d) {
        this.d = d;
    }

    /***
     * Method used to return the Double value of the object.
     * @return The double value
     */
    public Double getDouble() {
        return this.d;
    }

    /***
     * Method used to return the string of the DoubleWritable object
     * @return string of the DoubleWritable object
     */
    @Override
    public String getString() {
        return d.toString()+"\n";
    }

    /***
     * Method used to check quality between DoubleWritable objects.
     * @param o The other object to compare to
     * @return true or false based on the equality.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DoubleWritable that = (DoubleWritable) o;

        return !(d != null ? !d.equals(that.d) : that.d != null);

    }

    /***
     * Returns the hash code of the Double value.
     * @return Hash code of the double value.
     */
    @Override
    public int hashCode() {
        return d != null ? d.hashCode() : 0;
    }
}
