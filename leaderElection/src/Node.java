import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;

public class Node {
    private int id;
    private String host;
    private int port;
    private int round;
    private HashMap<Integer, Node> neighbors = new HashMap<Integer, Node>();

    private MsgService msgService;

    public Node(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.round = 0;
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getRound() {
        return round;
    }

    public HashMap<Integer, Node> getNeighbors() {
        return neighbors;
    }

    public void addNeighbor(int id, String host, int port) {
        this.neighbors.put(id, new Node(id, host, port));
    }

    public void startMsgService() throws Exception {
        MsgFactory.setLocalNodeId(id);
        msgService = new MsgService(this);
        msgService.startServer();

        Logger.Info("Msg Service started......");

        int count = 10;
        while (count > 0) {
            Logger.Info("%d seconds to start.", count);
            Thread.sleep(1000);
            count--;
        }

        connectNeighbors();

        Logger.Info("Connected to All neighbors....");

        while (!msgService.isInChannelsReady()) {
            Logger.Info("Waiting for in Channels ready....");
            Thread.sleep(1000);
        }
        waitForMessage();

        Logger.Info("Ready for messaging....");
    }

    public void connectNeighbors() throws IOException {
        msgService.startOutChannels();


    }

    public void waitForMessage() {
        msgService.registerEventListenser(new MsgEventListener() {

            @Override
            public void onReceiveMsg(Msg msg) {
                Logger.Info("onReceiveMsg: " + msg.toString());
                processMsg(msg);
            }
        });

        msgService.listenToChannels();
    }

    private void processMsg(Msg msg) {
        try {
            if (msg.getAction().equals(MsgAction.DISCONNECT)) {
                msgService.disconnect(msg.getFromId());
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public void sendTestMsg(String str) {
        Msg testMsg = MsgFactory.testMsg(this);
        testMsg.setContent(str);
        for (Node node : neighbors.values()) {
            testMsg.setToId(node.getId());
            msgService.sendMsg(testMsg);
        }
    }

    public void disconnect(int nodeId) throws IOException {
        msgService.disconnect(nodeId);
    }

}
