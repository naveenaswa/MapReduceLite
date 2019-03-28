package mapreduce;

/**
 * <h1>Text class</h1>
 * <h3>A class which represents String data type and is serializable</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class Text implements Writable, java.io.Serializable {

	private static final long serialVersionUID = -6038680120056221640L;
	String s;

    /***
     * Default constructor
     */
    public Text() {}

    /***
     * Cosntructor which creates a Text object from the given String object
     * @param s The String value
     */
    public Text(String s) {
        this.s = s;
    }

    /***
     * Method used to return the string of the Text object
     * @return string of the Text object
     */
    @Override
    public String getString() {
        return this.s+"\n";
    }

    /***
     * Method used to return the String value of the object.
     * @return The String value
     */
    public String get() {
        return this.s;
    }

    /***
     * Method used to set the String value of the Text object.
     * @param s The String value to be set
     */
    public void setText(String s) {
		this.s = s;
	}

    /***
     * Method used to check quality between Text objects.
     * @param o The other object to compare to
     * @return true or false based on the equality.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Text text = (Text) o;

        return !(s != null ? !s.equals(text.s) : text.s != null);

    }

    /***
     * Returns the hash code of the String value.
     * @return Hash code of the String value.
     */
    @Override
    public int hashCode() {
        return s != null ? s.hashCode() : 0;
    }
}
