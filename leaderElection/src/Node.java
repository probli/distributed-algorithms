import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

enum NodeState {
    ELECTING, ELECTED, IDLE, SEARCHING, WAITING, CONVERGING, CONVERGED, DONE
}

public class Node {
    private int id;
    private String host;
    private int port;
    private int round;
    private NodeState state;
    private boolean isLeader;
    private HashMap<Integer, Node> neighbors = new HashMap<>();
    private HashMap<Integer, Node> children = new HashMap<>();
    private int parent;
    private MsgService msgService;
    private int maxDegree;
    private int childrenMsgNo;
    private int largestUID;
    private int receivedLargestUID;
    private int distanceOfLargestUID;
    private int receivedDistanceOfLargestUID;
    private int unchangedRound;
    private int processedMsgNo;
    private int replyMsgNo;
    private static ConcurrentLinkedQueue<Msg> bufferedMsg = new ConcurrentLinkedQueue<>();

    public Node(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.round = 0;
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

        checkBuffer();

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
                processMsg(msg);
            }
        });
        msgService.listenToChannels();
    }

    private void processMsg(Msg msg) {
        try {

            if (msg.getRound() > getRound()) {
                addMsgToBuffer(msg);
                return;
            }

            if (msg.getAction().equals(MsgAction.DISCONNECT)) {
                // receive a msg to disconnect with the src-node.
                msgService.disconnect(msg.getFromId());

            } else if (msg.getAction().equals(MsgAction.ELECTLEADER)) {
                // Electing leader

                // receive a broadcast msg from leader
                if (msg.getContent().equals("LEADER")) {
                    leaderElected(msg);

                } else if (msg.getRound() == getRound()) {
                    // receive msg from nbs in the same round
                    Logger.Debug("Processing elect leader message...  %s", msg.toString());
                    String[] knowledge = msg.getContent().split(",");
                    int uid = Integer.parseInt(knowledge[0]);
                    int d = Integer.parseInt(knowledge[1]);
                    updateKnowledge(uid, d);
                    if (getProcessedMsgNo() == getRound() * neighbors.size() - 1) {
                        checkKnowledge();
                    }
                    updateProcessedMsgNo();
                }
            } else if (msg.getAction().equals(MsgAction.TEST)) {
                Logger.Debug("%s", msg.toString());

            } else if (msg.getAction().equals(MsgAction.SEARCH)) {
                int fromId = msg.getFromId();
                processSearchMsg(fromId);
                updateProcessedMsgNo();
            } else if (msg.getAction().equals(MsgAction.ACCEPT)) {
                int fromId = msg.getFromId();
                children.put(fromId, neighbors.get(fromId));
                updateReplyMsgNo();

            } else if (msg.getAction().equals(MsgAction.REJECT)) {
                updateReplyMsgNo();

            } else if (msg.getAction().equals(MsgAction.EMPTY)) {
                updateProcessedMsgNo();

            } else if (msg.getAction().equals(MsgAction.DEGREE)) {
                int d = this.getChildren().size() + (this.getParent() == this.getId() ? 0 : 1);
                this.maxDegree = Math.max(d, Math.max(this.maxDegree, Integer.parseInt(msg.getContent().trim())));
                updateChildrenMsgNo();
            } else if(msg.getAction().equals(MsgAction.END)) {
                broadcastToChildren(msg);
                setState(NodeState.DONE);
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public void leaderElected(Msg msg) {
        if (getState() == NodeState.ELECTED) {
            Logger.Debug("Leader %s Received", msg.getSrcId());
            setState(NodeState.IDLE);
            setIsLeader(false);
            transferMsg(msg);
        }
    }

    public synchronized void updateKnowledge(int uid, int d) {
        if (uid == getLargestUID()) {
            if (d > getReceivedDistanceOfLargestUID()) {
                setReceivedDistanceOfLargestUID(d);
            }
        } else if (uid > getLargestUID()) {
            if (uid >= getReceivedLargestUID()) {
                setReceivedLargestUID(uid);
                setReceivedDistanceOfLargestUID(d + 1);
            }
        }
    }

    public void checkKnowledge() {
        Logger.Debug("Round %s finished", getRound());
        if (getReceivedDistanceOfLargestUID() != getDistanceOfLargestUID() || getReceivedLargestUID() != getLargestUID()) {
            setUnchangedRound(0);
            setLargestUID(getReceivedLargestUID());
            setDistanceOfLargestUID(getReceivedDistanceOfLargestUID());
        } else if (getUnchangedRound() == 0) {
            setUnchangedRound(1);
        } else if (getState() == NodeState.ELECTING) {
            determineLeader();
        }
    }

    public void determineLeader() {
        if (id == getLargestUID()) {
            setState(NodeState.IDLE);
            setIsLeader(true);
            broadcastLeader();
        } else {
            setState(NodeState.ELECTED);
            setIsLeader(false);
        }
        Logger.Debug("leader election result: %s", getIsLeader());
    }

    public void broadcastLeader() {
        Msg electMsg = MsgFactory.electMsg(this);
        Logger.Debug("Leader MSG: %s", electMsg.getRound());
        electMsg.setContent("LEADER");
        broadcastMsg(electMsg);
    }

    public void transferMsg(Msg msg) {
        int from = msg.getFromId();
        for (Node node : neighbors.values()) {
            if (node.getId() != from) {
                msg.setToId(node.getId());
                msg.setFromId(id);
                msgService.sendMsg(msg);
            }
        }
    }

    public void broadcastMsg(Msg msg) {
        for (Node node : neighbors.values()) {
            msg.setToId(node.getId());
            msgService.sendMsg(msg);
        }
    }

    public void broadcastToChildren(Msg msg) {
        for (Node node : children.values()) {
            msg.setToId(node.getId());
            msgService.sendMsg(msg);
        }
    }

    public void disconnect(int nodeId) throws IOException {
        msgService.disconnect(nodeId);
    }

    public void leaderElectInit() {
        this.state = NodeState.ELECTING;
        this.round = 0;
        this.largestUID = id;
        this.receivedLargestUID = id;
        this.distanceOfLargestUID = 0;
        this.receivedDistanceOfLargestUID = 0;
        this.unchangedRound = 0;
        this.processedMsgNo = 0;
    }

    public void sendElectMsg() {
        Msg electMsg = MsgFactory.electMsg(this);
        Logger.Debug("Send MSG: %s", electMsg.getRound());
        broadcastMsg(electMsg);
    }

    public void sendTestMsg(String str) {
        Msg testMsg = MsgFactory.testMsg(this);
        testMsg.setContent(str);
        broadcastMsg(testMsg);
    }

    public void buildTreeInit() {
        this.state = NodeState.IDLE;
        this.round = 0;
        this.maxDegree = 0;
        this.parent = -1;
        this.processedMsgNo = 0;
        this.childrenMsgNo = 0;
        this.replyMsgNo = 0;
    }

    public void sendSearchMsg() {
        Msg msg = MsgFactory.searchMsg(this);
        broadcastMsg(msg);
    }

    public void sendAcceptMsg(int to) {
        Msg msg = MsgFactory.acceptMsg(this);
        msg.setToId(to);
        msgService.sendMsg(msg);
    }

    public void sendRejectMsg(int to) {
        Msg msg = MsgFactory.rejectMsg(this);
        msg.setToId(to);
        msgService.sendMsg(msg);
    }

    public void sendEmptyMsg() {
        Msg msg = MsgFactory.emptyMsg(this);
        broadcastMsg(msg);
    }

    public void sendDegreeMsg() {
        Msg msg = MsgFactory.degreeMsg(this);
        msg.setToId(this.parent);
        msg.setContent(String.valueOf(Math.max(this.children.size() + 1, this.maxDegree)));
        msgService.sendMsg(msg);
    }

    public void sendEndMsg() {
        Msg msg = MsgFactory.endMsg(this);
        broadcastMsg(msg);
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

    public void emptyMsgBuffer() {
        synchronized (bufferedMsg) {
            for (Msg m : bufferedMsg) {
                if (!m.getAction().equals(MsgAction.SEARCH)) {
                    bufferedMsg.remove(m);
                }
            }
        }
    }

    public void addMsgToBuffer(Msg msg) {
        synchronized (bufferedMsg) {
            bufferedMsg.add(msg);
        }
    }

    public void printMsgInBuffer() {
        synchronized (bufferedMsg) {
            for (Msg m : bufferedMsg) {
                Logger.Debug("Buffered Msg: %s", m.toString());
            }
        }
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

    public int getParent() {
        return parent;
    }

    public void setParent(int p) {
        parent = p;
    }

    public int getMaxDegree() {
        return maxDegree;
    }

    public int getChildrenMsgNo() {
        return childrenMsgNo;
    }

    public synchronized void updateReplyMsgNo() {
        this.replyMsgNo = this.replyMsgNo + 1;

        if (this.replyMsgNo == this.neighbors.size()) {
            setState(NodeState.CONVERGING);
        }
    }

    public void setRound(int r) {
        this.round = r;
    }

    public void updateRound() {
        this.round = this.round + 1;
    }

    public HashMap<Integer, Node> getNeighbors() {
        return neighbors;
    }

    public HashMap<Integer, Node> getChildren() {
        return children;
    }

    public void addNeighbor(int id, String host, int port) {
        this.neighbors.put(id, new Node(id, host, port));
    }

    public synchronized int getProcessedMsgNo() {
        return this.processedMsgNo;
    }

    public synchronized void updateProcessedMsgNo() {
        this.processedMsgNo = this.processedMsgNo + 1;
    }

    public synchronized int getLargestUID() {
        return this.largestUID;
    }

    public synchronized void setLargestUID(int uid) {
        this.largestUID = uid;
    }

    public synchronized int getReceivedLargestUID() {
        return this.receivedLargestUID;
    }

    public synchronized void setReceivedLargestUID(int uid) {
        this.receivedLargestUID = uid;
    }

    public synchronized int getDistanceOfLargestUID() {
        return this.distanceOfLargestUID;
    }

    public synchronized void setDistanceOfLargestUID(int d) {
        this.distanceOfLargestUID = d;
    }

    public synchronized int getReceivedDistanceOfLargestUID() {
        return this.receivedDistanceOfLargestUID;
    }

    public synchronized void setReceivedDistanceOfLargestUID(int d) {
        this.receivedDistanceOfLargestUID = d;
    }

    public synchronized int getUnchangedRound() {
        return this.unchangedRound;
    }

    public synchronized void setUnchangedRound(int r) {
        this.unchangedRound = r;
    }

    public synchronized NodeState getState() {
        return this.state;
    }

    public synchronized void setState(NodeState s) {
        Logger.Debug(this.state + " --- > " + s);
        this.state = s;
    }

    public synchronized boolean getIsLeader() {
        return this.isLeader;
    }

    public synchronized void setIsLeader(boolean s) {
        this.isLeader = s;
    }

    public synchronized void updateChildrenMsgNo() {
        this.childrenMsgNo = this.childrenMsgNo + 1;
    }

    public synchronized void processSearchMsg(int pId) {
        if (getState() == NodeState.IDLE) {
            setState(NodeState.SEARCHING);
            setParent(pId);
            sendAcceptMsg(pId);
        } else {
            sendRejectMsg(pId);
        }
    }
}
