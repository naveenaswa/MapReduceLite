package mapreduce;

/**
 * <h1>FloatWritable class</h1>
 * <h3>A class which represents Float data type and is serializable</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class FloatWritable implements Writable, java.io.Serializable {

	private static final long serialVersionUID = -5304458904116339903L;
	Float f;

    /***
     * Default constructor
     */
    public FloatWritable() {}

    /***
     * Cosntructor which creates a FloatWritable object from the given Float object
     * @param f The double value
     */
    public FloatWritable(Float f) {
        this.f = f;
    }

    /***
     * Method used to return the Float value of the object.
     * @return The float value
     */
    public Float getFloat() {
        return this.f;
    }

    /***
     * Method used to return the string of the FloatWritable object
     * @return string of the FloatWritable object
     */
    @Override
    public String getString() {
        return f.toString()+"\n";
    }

    /***
     * Method used to check quality between FloatWritable objects.
     * @param o The other object to compare to
     * @return true or false based on the equality.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FloatWritable that = (FloatWritable) o;

        return !(f != null ? !f.equals(that.f) : that.f != null);

    }

    /***
     * Returns the hash code of the Float value.
     * @return Hash code of the float value.
     */
    @Override
    public int hashCode() {
        return f != null ? f.hashCode() : 0;
    }
}
