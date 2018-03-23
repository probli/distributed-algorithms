import java.io.*;
import java.net.Socket;

public class MsgChannel {
    private boolean isConnecting = false;

    private BufferedReader in = null;
    private PrintStream out = null;

    private int nodeId;
    private String host;
    private int port;
    private Socket clientSocket;
    private Socket serverSocket;

    public MsgChannel(int id, String h, int p) throws IOException {
        nodeId = id;
        host = h;
        port = p;
    }

    public boolean isConnecting() {
        return isConnecting;
    }

    public BufferedReader getInChannel() {
        return in;
    }

    public PrintStream getOutChannel() {
        return out;
    }

    public boolean hasInChannel() {
        return in == null ? false : true;
    }

    public void assignInChannel(Socket st, BufferedReader in) {
        this.serverSocket = st;
        this.in = in;
    }

    public boolean hasOutChannel() {
        return out == null ? false : true;
    }

    public boolean connectOutChannel() {

        try {
            clientSocket = new Socket(host, port);
            out = new PrintStream(clientSocket.getOutputStream());
            out.println(MsgFactory.connectMsg(nodeId));
            return true;
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
            return false;
        }
    }

    public void shutdownInChannel() {
        try {
            if (!this.serverSocket.isClosed()) {
                this.in.close();
                this.serverSocket.close();
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public void setConnecting() {
        isConnecting = true;
    }

    public void disconnect() throws IOException {
        isConnecting = false;
        out.println(MsgFactory.disconnectMsg(nodeId));
        out.close();
        clientSocket.close();
    }


}
