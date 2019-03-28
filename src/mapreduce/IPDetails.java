package mapreduce;

/**
 * <h1>IPDetails class</h1>
 * <h3>A class which represents IP address and port number of all the nodes in the cluster.</h3>
 * @author Ruinan, Karthik, Sujith, Naveen
 * @version 1.0
 * @since   2016-04-20
 */
public class IPDetails {
    private String ipAddr;
    private String port;

    /***
     * The constructor which instantiates the IPDetails object with the IP address and port
     * @param ipAddr The IP address of the node
     * @param port The port of the node
     */
    public IPDetails(String ipAddr, String port) {
        this.ipAddr = ipAddr;
        this.port = port;
    }

    /***
     * Returns the IP address of the node.
     * @return IP address
     */
    public String getIpAddr() {
        return ipAddr;
    }

    /***
     * Sets the IP address of the node
     * @param ipAddr The IP address to be set
     */
    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    /***
     * Returns the port of the node.
     * @return port
     */
    public String getPort() {
        return port;
    }

    /***
     * Sets the port of the node
     * @param port The port to be set
     */
    public void setPort(String port) {
        this.port = port;
    }

    /***
     * Method used to check quality between IPDetails objects.
     * @param o The other object to compare to
     * @return true or false based on the equality.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IPDetails ipDetails = (IPDetails) o;

        if (ipAddr != null ? !ipAddr.equals(ipDetails.ipAddr) : ipDetails.ipAddr != null) return false;
        return !(port != null ? !port.equals(ipDetails.port) : ipDetails.port != null);

    }

}
