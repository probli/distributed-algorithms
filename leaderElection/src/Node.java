import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

enum NodeState {
    IDLE, ELECT, BUILDTREE
}
enum ElectState {
    UNKNOWN, ISLEADER, ISNOTLEADER
}

enum BuildTreeState {
    WAITING, MARKED, DONE
}
public class Node {
    private int id;
    private String host;
    private int port;
    private MsgService msgService;
    private HashMap<Integer, Node> neighbors = new HashMap<>();

    private static ConcurrentLinkedQueue<Msg> bufferedMsg = new ConcurrentLinkedQueue<>();

    private NodeState nodeState;
    private int round;

    private int largestUID;
    private int receivedLargestUID;
    private int distanceOfLargestUID;
    private int receivedDistanceOfLargestUID;
    private int unchangedRound;
    private int processedMsgNoElect;

    private boolean isMarked;
    private ElectState electState;
    private BuildTreeState buildTreeState;
    private int processedMsgNoBuild;
    private int replyMsgNo;
    private int childrenMsgNo;
    private HashMap<Integer, Node> children = new HashMap<>();
    private int maxDegree;
    private int parent;
    

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
            if (msg.getRound() != getRound() && msg.getRound() != -1) {
                addMsgToBuffer(msg);
                return;
            }

            if (msg.getAction().equals(MsgAction.DISCONNECT)) {
                msgService.disconnect(msg.getFromId());
            } else if (msg.getAction().equals(MsgAction.ELECTLEADER)) {
                // Electing leader
                // receive a broadcast msg from leader
                if (msg.getContent().equals("LEADER")) {
                    leaderElected(msg);
                } else if (msg.getRound() == getRound()) {
                    // receive msg from nbs in the same round
                    // Logger.Debug("Processing elect leader message...  %s", msg.toString());
                    String[] knowledge = msg.getContent().split(",");
                    int uid = Integer.parseInt(knowledge[0]);
                    int d = Integer.parseInt(knowledge[1]);
                    updateKnowledge(uid, d);
                    if (getProcessedMsgNoElect() == getRound() * neighbors.size() - 1) {
                        checkKnowledge();
                    }
                    updateProcessedMsgNoElect();
                }
            } else if (msg.getAction().equals(MsgAction.BUILD)) {
                if (msg.getContent().equals("SEARCH")) {
                    int fromId = msg.getFromId();
                    processSearchMsg(fromId);
                }
                updateProcessedMsgNoBuild();
            } else if (msg.getAction().equals(MsgAction.REPLY)) {
                if (msg.getContent().equals("ACCEPT")) {
                    int fromId = msg.getFromId();
                    children.put(fromId, neighbors.get(fromId));
                }
                updateReplyMsgNo();
                checkConverge();
            } else if (msg.getAction().equals(MsgAction.TEST)) {
                Logger.Debug("%s", msg.toString());
            } else if (msg.getAction().equals(MsgAction.DEGREE)) {
                int d = this.getChildren().size() + (this.getParent() == this.getId() ? 0 : 1);
                this.maxDegree = Math.max(d, Math.max(this.maxDegree, Integer.parseInt(msg.getContent().trim())));
                updateChildrenMsgNo();
                checkConverge();
           } else if (msg.getAction().equals(MsgAction.END)) {
                broadcastToChildren(msg);
                setBuildTreeState(BuildTreeState.DONE);
            } else {
                Logger.Debug(String.format("[!!!!Lost!!!!] %s | s: %d, f: %d, t: %d, r: %d, c: %s, STATE: %s", msg.getAction(), msg.getSrcId(), msg.getFromId(), msg.getToId(), msg.getRound(), msg.getContent(), getNodeState()));
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public void leaderElected(Msg msg) {
        if (getNodeState() == NodeState.ELECT) {
            if (getNodeState() == NodeState.ELECT) {
                setNodeState(NodeState.IDLE);
            }
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
        if (getReceivedDistanceOfLargestUID() != getDistanceOfLargestUID() || getReceivedLargestUID() != getLargestUID()) {
            setUnchangedRound(0);
            setLargestUID(getReceivedLargestUID());
            setDistanceOfLargestUID(getReceivedDistanceOfLargestUID());
        } else if (getUnchangedRound() == 0) {
            setUnchangedRound(1);
        } else if (getElectState().equals(ElectState.UNKNOWN)) {
            determineLeader();
        }
    }

    public void determineLeader() {
        if (id == getLargestUID()) {
            setElectState(ElectState.ISLEADER);
            setNodeState(NodeState.IDLE);
            broadcastLeader();
        } else {
            setElectState(ElectState.ISNOTLEADER);
        }
    }

    public void broadcastLeader() {
        Msg electMsg = MsgFactory.electMsg(this);
        Logger.Debug("Leader MSG: %s", electMsg.getRound());
        electMsg.setContent("LEADER");
        electMsg.setRound(-1);
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
        this.nodeState = NodeState.ELECT;
        this.electState = ElectState.UNKNOWN;
        this.round = 0;
        this.largestUID = id;
        this.receivedLargestUID = id;
        this.distanceOfLargestUID = 0;
        this.receivedDistanceOfLargestUID = 0;
        this.unchangedRound = 0;
        this.processedMsgNoElect = 0;
    }

    public void sendElectMsg() {
        Msg electMsg = MsgFactory.electMsg(this);
        broadcastMsg(electMsg);
    }

    public void sendTestMsg(String str) {
        Msg testMsg = MsgFactory.testMsg(this);
        testMsg.setContent(str);
        broadcastMsg(testMsg);
    }

    public void buildTreeInit() {
        this.nodeState = NodeState.BUILDTREE;
        this.buildTreeState = BuildTreeState.WAITING;
        this.isMarked = false;
        this.round = 0;
        this.maxDegree = 0;
        this.parent = -1;
        this.processedMsgNoBuild = 0;
        this.childrenMsgNo = 0;
        this.replyMsgNo = 0;
    }

    public void markLeader() {
        if (this.electState == ElectState.ISLEADER) {
            setBuildTreeState(BuildTreeState.MARKED);
            setParent(id);
            markNode();
        }
    }

    public void markNode() {
        this.isMarked = true;
    }

    public synchronized void processSearchMsg(int pId) {
        if (!getIsMarked()) {
            setBuildTreeState(BuildTreeState.MARKED);
            markNode();
            setParent(pId);
            sendAcceptMsg(pId);
        } else {
            sendRejectMsg(pId);
        }
    }
    
    public synchronized void checkConverge() {
        if (getReplyMsgNo() == this.neighbors.size() && getChildrenMsgNo() == this.children.size()) {
            if (getElectState() == ElectState.ISNOTLEADER) {
                sendDegreeMsg();
            } else if (getElectState() == ElectState.ISLEADER) {
                setBuildTreeState(BuildTreeState.DONE);
                sendEndMsg();
            }
        }
    }

    public boolean getIsMarked() {
        return this.isMarked;
    }

    public void sendSearchMsg() {
        Msg msg = MsgFactory.buildMsg(this, "SEARCH");
        broadcastMsg(msg);
    }

    public void sendEmptyMsg() {
        Msg msg = MsgFactory.buildMsg(this, "EMPTY");
        broadcastMsg(msg);
    }
    
    public void sendAcceptMsg(int to) {
        Msg msg = MsgFactory.replyMsg(this, "ACCEPT", to);
        msgService.sendMsg(msg);
    }

    public void sendRejectMsg(int to) {
        Msg msg = MsgFactory.replyMsg(this, "REJECT", to);
        msgService.sendMsg(msg);
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

    public void addMsgToBuffer(Msg msg) {
        synchronized (bufferedMsg) {
            bufferedMsg.add(msg);
        }
    }

    public void printMsgInBuffer() {
        synchronized (bufferedMsg) {
            for (Msg m : bufferedMsg) {
                Logger.Info("Buffered Msg: %s", m.toString());
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

    public synchronized int getChildrenMsgNo() {
        return childrenMsgNo;
    }

    public synchronized void updateReplyMsgNo() {
        this.replyMsgNo = this.replyMsgNo + 1;
    }

    public synchronized int getReplyMsgNo() {
        return this.replyMsgNo;
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

    public synchronized int getProcessedMsgNoElect() {
        return this.processedMsgNoElect;
    }

    public synchronized int getProcessedMsgNoBuild() {
        return this.processedMsgNoBuild;
    }

    public synchronized void updateProcessedMsgNoElect() {
        this.processedMsgNoElect = this.processedMsgNoElect + 1;
    }

    public synchronized void updateProcessedMsgNoBuild() {
        this.processedMsgNoBuild = this.processedMsgNoBuild + 1;
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

    public synchronized NodeState getNodeState() {
        return this.nodeState;
    }

    public synchronized void setNodeState(NodeState s) {
        Logger.Info("Round %s : %s ----> %s", getRound(),this.nodeState, s);
        this.nodeState = s;
    }
    
    public synchronized ElectState getElectState() {
        return this.electState;
    }

    public synchronized void setElectState(ElectState es) {
        Logger.Info("Round %s : %s ----> %s", getRound(), this.electState, es);
        this.electState = es;
    }
    
    public synchronized BuildTreeState getBuildTreeState() {
        return this.buildTreeState;
    }

    public synchronized void setBuildTreeState(BuildTreeState bts) {
        Logger.Info("Round %s : %s ----> %s", getRound(), this.buildTreeState, bts);
        this.buildTreeState = bts;
    }

    public synchronized void updateChildrenMsgNo() {
        this.childrenMsgNo = this.childrenMsgNo + 1;
    }

}
