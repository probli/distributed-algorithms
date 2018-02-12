import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

enum leaderElectStatus {
    UNKNOWN, ISNOTLEADER, ISLEADER
}

enum state {
    HOLD, ELECTLEADER
}

public class Node {
    private int id;
    private String host;
    private int port;
    private int round;
    private state nodeState;
    private HashMap<Integer, Node> neighbors = new HashMap<Integer, Node>();

    private MsgService msgService;

    private leaderElectStatus pelegStatus;
    private int largestUID;
    private int nextLargestUID;
    private int distanceOfLargestUID;
    private int nextDistanceOfLargestUID;
    private int unchangedRound;
    private int processedMsgNo;

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
                // Logger.Info("onReceiveMsg: " + msg.toString());
                processMsg(msg);
            }
        });
        msgService.listenToChannels();
    }

    private void processMsg(Msg msg) {
        try {
            if (msg.getAction().equals(MsgAction.DISCONNECT)) {
                msgService.disconnect(msg.getFromId());
            } else if (msg.getAction().equals(MsgAction.ELECTLEADER)) {
                if (msg.getContent().equals("LEADER")) {
                    leaderElected(msg);
                } else if(msg.getRound() == getRound()) {
                    Logger.Debug("Processing...  %s", msg.toString());
                    String[] knowledge = msg.getContent().split(",");
                    int uid = Integer.parseInt(knowledge[0]);
                    int d = Integer.parseInt(knowledge[1]);
                    updateKnowledge(uid, d);
                    if (getProcessedMsgNo() == getRound() * neighbors.size() - 1) {
                        checkKnowledge();
                    }
                    updateProcessedMsgNo();
                } else {
                    addMsgToBuffer(msg);
                }
            } else if(msg.getAction().equals(MsgAction.TEST)) {
                Logger.Debug("%s", msg.toString());
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public void leaderElected(Msg msg) {
        if (getNodeState() == state.ELECTLEADER) {
            Logger.Debug("Leader %s Recieved", msg.getSrcId());
            setNodeState(state.HOLD);
            transferMsg(msg);
        }
    }

    public synchronized void updateKnowledge(int uid, int d) {
        if (uid == getLargestUID()) {
            if (d > getNextDistanceOfLargestUID()) {
                setNextDistanceOfLargestUID(d);
            }
        } else if (uid > getLargestUID()) {
            if (uid >= getNextLargestUID()) {
                setnextLargestUID(uid);
                setNextDistanceOfLargestUID(d + 1);
            }
        }
    }

    public void checkKnowledge() {
        Logger.Debug("Round %s finished", getRound());
        if (getNextDistanceOfLargestUID() != getDistanceOfLargestUID() || getNextLargestUID() != getLargestUID()) {
            setUnchangedRound(0);
            setLargestUID(getNextLargestUID());
            setDistanceOfLargestUID(getNextDistanceOfLargestUID());
        } else if (getUnchangedRound() == 0) {
            setUnchangedRound(1);
        } else if (getPelegStatus() == leaderElectStatus.UNKNOWN){
            determineLeader();
        }
    }

    public void determineLeader() {
        if (id == getLargestUID()) {
            setPelegStatus(leaderElectStatus.ISLEADER);
            broadcastLeader();
        } else {
            setPelegStatus(leaderElectStatus.ISNOTLEADER);
        }
        Logger.Debug("leader election result: %s", getPelegStatus());
    }

    public void broadcastLeader() {
        setNodeState(state.HOLD);
        String msgContent = "LEADER";
        sendElectMsg(msgContent);
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

    public void disconnect(int nodeId) throws IOException {
        msgService.disconnect(nodeId);
    }

    public void leaderElectInit() {
        this.nodeState = state.ELECTLEADER;
        this.round = 0;
        this.largestUID = id;
        this.nextLargestUID = id;
        this.distanceOfLargestUID = 0;
        this.nextDistanceOfLargestUID = 0;
        this.pelegStatus = leaderElectStatus.UNKNOWN;
        this.unchangedRound = 0;
        this.processedMsgNo = 0;
    }

    public void sendElectMsg(String str) {
        Msg electMsg = MsgFactory.electMsg(this);
        electMsg.setContent(str);
        Logger.Debug("Send MSG: %s", electMsg.getRound());
        broadcastMsg(electMsg);
    }

    public void sendTestMsg(String str) {
        Msg testMsg = MsgFactory.testMsg(this);
        testMsg.setContent(str);
        broadcastMsg(testMsg);
    }

    public void checkBuffer() {
        (new Thread() {
            @Override
            public void run(){
                while (true) {
                    while (!bufferedMsg.isEmpty()) {
                        synchronized(bufferedMsg){
                            for (Msg m : bufferedMsg) {
                                if (m.getRound() == getRound()){
                                    processMsg(m);
                                    bufferedMsg.remove(m);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }).start();
    }

    public void emptyMsgBuffer() {
        synchronized(bufferedMsg){
            bufferedMsg.clear();
        }
    }

    public void addMsgToBuffer(Msg msg) {
        synchronized(bufferedMsg) {
            bufferedMsg.add(msg);
        }
    }

    public void printMsgInBuffer() {
        synchronized(bufferedMsg) {
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

    public void setRound(int r) {
        this.round = r;
    }
    public void updateRound() {
        this.round = this.round + 1;
    }

    public HashMap<Integer, Node> getNeighbors() {
        return neighbors;
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

    public leaderElectStatus getPelegStatus() {
        return this.pelegStatus;
    }

    public synchronized void setPelegStatus(leaderElectStatus status) {
        this.pelegStatus = status;
    }

    public synchronized int getLargestUID() {
        return this.largestUID;
    }

    public synchronized void setLargestUID(int uid) {
        this.largestUID = uid;
    }

    public synchronized int getNextLargestUID() {
        return this.nextLargestUID;
    }

    public synchronized void setnextLargestUID(int uid) {
        this.nextLargestUID = uid;
    }

    public synchronized int getDistanceOfLargestUID() {
        return this.distanceOfLargestUID;
    }

    public synchronized void setDistanceOfLargestUID(int d) {
        this.distanceOfLargestUID = d;
    }

    public synchronized int getNextDistanceOfLargestUID() {
        return this.nextDistanceOfLargestUID;
    }

    public synchronized void setNextDistanceOfLargestUID(int d) {
        this.nextDistanceOfLargestUID = d;
    }

    public synchronized int getUnchangedRound() {
        return this.unchangedRound;
    }

    public synchronized void setUnchangedRound(int r) {
        this.unchangedRound = r;
    }

    public synchronized state getNodeState() {
        return this.nodeState;
    }

    public synchronized void setNodeState(state s) {
        this.nodeState = s;
    }
}
