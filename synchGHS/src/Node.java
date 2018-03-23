import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

enum NodeState {
    IDLE, ELECT, BUILDTREE
}

public class Node {
    private int id;
    private String host;
    private int port;
    private HashMap<Integer, Node> neighbors = new HashMap<>();
    private TreeMap<Integer, Integer> edges = new TreeMap<>();

    private MsgService msgService;
    private static ConcurrentLinkedQueue<Msg> bufferedMsg = new ConcurrentLinkedQueue<>();
    private int round;

    private NodeState nodeState;

    public Node(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public void startMsgService() throws Exception {
        MsgFactory.setLocalNodeId(id);
        msgService = new MsgService(this);
        msgService.startServer();
        Logger.Info("Msg Service started......");

        int count = 5;
        while (count > 0) {
            Logger.Info("%d seconds to start.", count);
            Thread.sleep(1000);
            count--;
        }

        connectNeighbors();
        Logger.Info("Connected to All neighbors....");

        while (!msgService.isInChannelsReady()) {
            StringBuilder sb = new StringBuilder("Current InChannels: ");
            for(int id : msgService.channels.keySet()) {
                MsgChannel ch = msgService.channels.getOrDefault(id, null);
                if(ch != null && ch.hasInChannel()) {
                    sb.append(id + ", ");
                }
            }
            Logger.Info(sb.toString());
            Thread.sleep(1000);
        }

        checkBuffer();

        waitForMessage();
        Logger.Info("Ready for messaging....");
    }

    public void connectNeighbors() throws IOException {
        msgService.startOutChannels();
    }

    public void disconnect(int nodeId) throws IOException {
        msgService.disconnect(nodeId);
    }

    public void waitForMessage() {
        msgService.registerEventListenser(new MsgEventListener() {
            @Override
            public void onReceiveMsg(Msg msg) {
                processMsg(msg);
            }
        });
        msgService.listenToChannels();
    }

    public void checkBuffer() {
        Runnable task = () -> {
            while (true) {
                while (!bufferedMsg.isEmpty()) {
                    synchronized (bufferedMsg) {
                        for (Msg m : bufferedMsg) {
                            if (m.getRound() == getRound()) {
                                processMsg(m);
                                bufferedMsg.remove(m);
                                break;
                            }
                        }
                    }
                }
            }
        };
        new Thread(task).start();
    }

    public void addMsgToBuffer(Msg msg) {
        synchronized (bufferedMsg) {
            bufferedMsg.add(msg);
        }
    }

    private void processMsg(Msg msg) {
        try {
            if (msg.getRound() != getRound() && msg.getRound() != -1) {
                addMsgToBuffer(msg);
                return;
            }

            if (msg.getAction().equals(MsgAction.DISCONNECT)) {
                msgService.disconnect(msg.getFromId());
            } else if (msg.getAction().equals(MsgAction.TEST)) {
                Logger.Debug("%s", msg.toString());
            } else {
                Logger.Debug(String.format("[!!!!Lost!!!!] %s, STATE: %s", msg.printFormat(), getNodeState()));
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public void broadcastMsg(Msg msg) {
        for (Node node : neighbors.values()) {
            msg.setToId(node.getId());
            msgService.sendMsg(msg);
        }
    }

    public void sendTestMsg(String str) {
        Msg testMsg = MsgFactory.testMsg(this);
        testMsg.setContent(str);
        broadcastMsg(testMsg);
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

    public HashMap<Integer, Node> getNeighbors() {
        return neighbors;
    }

    public void addNeighbor(int id, String host, int port, int weight) {
        this.neighbors.put(id, new Node(id, host, port));
        this.edges.put(id, weight);
    }

    public int getRound() {
        return round;
    }

    public synchronized NodeState getNodeState() {
        return this.nodeState;
    }

    public synchronized void setNodeState(NodeState s) {
        Logger.Info("Round %s : %s ----> %s", getRound(),this.nodeState, s);
        this.nodeState = s;
    }

}
