import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

enum NodeState {
    IDLE, SEARCH, TEST, CONVERGE, MERGE, JOIN, TERMINATE
}

public class Node {
    private int N;
    private int id;
    private String host;
    private int port;
    private HashMap<Integer, Node> neighbors = new HashMap<>();
    private PriorityQueue<Edge> edges = new PriorityQueue<>();

    private MsgService msgService;
    private ConcurrentLinkedQueue<Msg> bufferedMsg = new ConcurrentLinkedQueue<>();

    private NodeState nodeState;
    private int round;
    private int searchRound;
    private int convergeRound;
    private int mergeRound;
    private int searchMsgNo;
    private int convergeMsgNo;
    private int mergeMsgNo;

    private int componentId;
    private Integer newComponentId;
    private boolean isLeader = true;;
    private List<Edge> treeEdges = new LinkedList<>();
    private HashMap<Integer, Node> treeNeighbors = new HashMap<>();
    private List<Edge> newTreeEdges = new LinkedList<>();
    private HashMap<Integer, Node> newTreeNeighbors = new HashMap<>();
    private int totalJoinMsg;
    private Edge MWOE;
    private Edge localMWOE;
    private int childrenMsgNo;
    private int joinMsgNo = 0;
    private int componentLevel;
    private Node parent;
    private boolean hasGlobalMWOE;

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
            for (int id : msgService.channels.keySet()) {
                MsgChannel ch = msgService.channels.getOrDefault(id, null);
                if (ch != null && ch.hasInChannel()) {
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
                            if (this.getComponentLevel() == m.getComponentLevel()
                                    && (m.getRound() <= getRound() || m.getRound() == -1)) {
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
            if (msg.getComponentLevel() > this.getComponentLevel()
                    || (msg.getRound() > this.getRound() && msg.getRound() != -1)) {
                addMsgToBuffer(msg);
                return;
            }

            if (msg.getAction().equals(MsgAction.DISCONNECT)) {
                msgService.disconnect(msg.getFromId());
            } else if (msg.getAction().equals(MsgAction.SEARCH)) {
                String content = msg.getContent();
                if (content.equals("SEARCH")) {
                    int fromId = msg.getFromId();
                    int srcId = msg.getSrcId();
                    processSearchMsg(fromId, srcId);
                }

                updateSearchMsgNo();
            } else if (msg.getAction().equals(MsgAction.TEST)) {
                int fromId = msg.getFromId();
                int receivedId = Integer.parseInt(msg.getContent());
                processTestMsg(fromId, receivedId);
            } else if (msg.getAction().equals(MsgAction.REPLY)) {
                String content = msg.getContent();
                processReplyMsg(content);
            } else if (msg.getAction().equals(MsgAction.CONVERGE)) {
                String content = msg.getContent();
                if (!content.equals("EMPTY")) {
                    processConvergeMsg(content);
                    updateChildrenMsgNo();
                    checkConverge();
                }
                updateConvergeMsgNo();
            } else if (msg.getAction().equals(MsgAction.MERGE)) {
                String content = msg.getContent();
                if (!content.equals("EMPTY")) {
                    int fromId = msg.getFromId();
                    int srcId = msg.getSrcId();
                    processMergeMsg(fromId, srcId, content);
                }
                updateMergeMsgNo();
            } else if (msg.getAction().equals(MsgAction.JOIN)) {
                int fromId = msg.getFromId();
                String content = msg.getContent();
                processJoinMsg(fromId, content);
            } else if (msg.getAction().equals(MsgAction.TERMINATE)) {
                if (this.nodeState == NodeState.TERMINATE) {
                    return;
                }
                transferToChildren(msg);
                setNodeState(NodeState.TERMINATE);
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

    private synchronized void processSearchMsg(int fromId, int srcId) {
        if (parent == null && srcId != id) {
            setParent(fromId);
            setComponentId(srcId);
            Logger.Debug("This Component level is: %s", this.getComponentLevel());
            Logger.Debug("COMPONENT ID IS %s", this.getComponentId());
            setNodeState(NodeState.SEARCH);
        }
    }

    private synchronized void updateSearchMsgNo() {
        this.searchMsgNo = this.searchMsgNo + 1;
    }

    private synchronized void updateConvergeMsgNo() {
        this.convergeMsgNo = this.convergeMsgNo + 1;
    }

    private synchronized void updateMergeMsgNo() {
        this.mergeMsgNo = this.mergeMsgNo + 1;
    }

    private synchronized int getSearchMsgNo() {
        return this.searchMsgNo;
    }

    private synchronized int getConvergedMsgNo() {
        return this.convergeMsgNo;
    }

    private synchronized int getMergeMsgNo() {
        return this.mergeMsgNo;
    }

    private synchronized void updateChildrenMsgNo() {
        this.childrenMsgNo = this.childrenMsgNo + 1;
    }

    private synchronized void updateJoinMsgNo() {
        this.joinMsgNo = this.joinMsgNo + 1;
        Logger.Debug("Join message is %s", this.joinMsgNo);
    }

    private synchronized int getJoinMsgNo() {
        return this.joinMsgNo;
    }

    private synchronized void processTestMsg(int fromId, int receivedId) {
        if (receivedId != this.getComponentId()) {
            sendReplyMsg(fromId, "ACCEPT");
        } else {
            sendReplyMsg(fromId, "REJECT");
        }
    }

    private synchronized void processReplyMsg(String content) {
        Edge mwoe = edges.poll();
        if (content.equals("ACCEPT")) {
            setMWOE(mwoe);
            this.localMWOE = mwoe;
        } else if (content.equals("REJECT")) {
            setNodeState(NodeState.TEST);
        }
    }

    private synchronized void processConvergeMsg(String content) {
        Edge mwoe;
        if (content.isEmpty()) {
            mwoe = null;
        } else {
            String[] edge = content.split(",");
            int ep1 = Integer.parseInt(edge[0]);
            int ep2 = Integer.parseInt(edge[1]);
            int weight = Integer.parseInt(edge[2]);

            mwoe = new Edge(ep1, ep2, weight);
        }
        int res = compare(MWOE, mwoe);
    
        if (res > 0) {
            setMWOE(mwoe);
        }
    }

    private synchronized void processMergeMsg(int fromId, int srcId, String content) {
        if (parent == null && srcId != id) {
            String[] edge = content.split(",");
            int ep1 = Integer.parseInt(edge[0]);
            int ep2 = Integer.parseInt(edge[1]);
            int weight = Integer.parseInt(edge[2]);
            Edge mwoe = new Edge(ep1, ep2, weight);
            setMWOE(mwoe);
            if (ep1 == id || ep2 == id) {
                this.hasGlobalMWOE = true;
            }
            setParent(fromId);
            setNodeState(NodeState.MERGE);
        }
    }

    private synchronized void processJoinMsg(int fromId, String content) {
        updateJoinMsgNo();
        if (this.joinMsgNo == 2 * this.totalJoinMsg) {
            setNodeState(NodeState.IDLE);
        }
        if (content.equals("EMPTY")) {
            return;
        }
        String[] edge = content.split(",");
        int ep1 = Integer.parseInt(edge[0]);
        int ep2 = Integer.parseInt(edge[1]);
        int weight = Integer.parseInt(edge[2]);
        Edge mwoe = new Edge(ep1, ep2, weight);

        int res = compare(MWOE, mwoe);
        if (hasGlobalMWOE && res == 0) {
            setNewComponentId(Math.max(ep1, ep2));
        } else {
            updateTreeNeighbors(fromId, mwoe);
        }
    }

    public synchronized void updateTreeNeighbors(int id, Edge edge) {

        if (!newTreeNeighbors.containsKey(id)) {
            Node newTreeNeighbor = neighbors.get(id);
            newTreeNeighbors.put(id, newTreeNeighbor);
            newTreeEdges.add(edge);
        }

    }

    private synchronized void transferToChildren(Msg msg) {
        for (Node node : treeNeighbors.values()) {
            if (msg.getFromId() != node.getId()) {
                msg.setToId(node.getId());
                msgService.sendMsg(msg);
            }
        }
    }

    public int compare(Edge e1, Edge e2) {
        if (e1 == null)
            return 1;
        return e1.compareTo(e2);
    }

    public void setN(int n) {
        this.N = n;
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

    public PriorityQueue<Edge> getEdges() {
        return edges;
    }

    public void addNeighbor(int id, String host, int port, int weight) {
        this.neighbors.put(id, new Node(id, host, port));
        this.edges.offer(new Edge(this.id, id, weight));
    }

    public synchronized int getRound() {
        return round;
    }

    public synchronized int getSearchRound() {
        return searchRound;
    }

    public synchronized int getConvergeRound() {
        return convergeRound;
    }

    public synchronized int getMergeRound() {
        return mergeRound;
    }

    public synchronized void updateRound() {
        this.round = this.round + 1;
        Logger.Debug("Current round is %s", this.round);
    }

    public synchronized void updateSearchRound() {
        this.searchRound = this.searchRound + 1;
        Logger.Debug("Current search round is %s", this.searchRound);
    }

    public synchronized void updateConvergeRound() {
        this.convergeRound = this.convergeRound + 1;
        Logger.Debug("Current converge round is %s", this.convergeRound);
    }

    public synchronized void updateMergeRound() {
        this.mergeRound = this.mergeRound + 1;
        Logger.Debug("Current merge round is %s", this.mergeRound);
    }

    public void setRound(int r) {
        this.round = r;
    }

    public synchronized NodeState getNodeState() {
        return this.nodeState;
    }

    public synchronized void setNodeState(NodeState s) {
        Logger.Info("Round %s : %s ----> %s", getRound(), this.nodeState, s);
        this.nodeState = s;
    }

    public synchronized int getComponentId() {
        return this.componentId;
    }

    public synchronized void setComponentId(int c) {
        this.componentId = c;
    }

    public synchronized int getNewComponentId() {
        return this.newComponentId;
    }

    public synchronized void setNewComponentId(int nc) {
        this.newComponentId = nc;
    }

    public synchronized int getComponentLevel() {
        return this.componentLevel;
    }

    public synchronized void setComponentLevel(int cl) {
        this.componentLevel = cl;
    }

    public synchronized void updateComponentLevel() {
        Logger.Debug("[Component Level] %s --> %s", this.componentLevel, this.componentLevel + 1);
        this.componentLevel = this.componentLevel + 1;
    }

    public List<Edge> getTreeEdges() {
        return this.treeEdges;
    }

    public void addTreeEdge(Edge e) {
        this.treeEdges.add(e);
    }

    public HashMap<Integer, Node> getTreeNeighbors() {
        return this.treeNeighbors;
    }

    public void addTreeNeighbors(int id) {
        Node treeNeighbor = neighbors.get(id);
        this.treeNeighbors.put(id, treeNeighbor);
    }

    public synchronized Edge getMWOE() {
        return MWOE;
    }

    public synchronized void setMWOE(Edge e) {
        this.MWOE = e;
    }

    public Node getParent() {
        return this.parent;
    }

    public void setParent(int id) {
        Node par = this.neighbors.get(id);
        this.parent = par;
    }

    public void initBuildMST() {
        this.componentId = this.id;
        this.nodeState = NodeState.IDLE;
        this.componentLevel = 0;
        this.round = 0;
        this.searchRound = 0;
        this.convergeRound = 0;
        this.mergeRound = 0;
        this.searchMsgNo = 0;
        this.convergeMsgNo = 0;
        this.mergeMsgNo = 0;
        this.MWOE = null;
        this.parent = null;
        this.hasGlobalMWOE = false;
    }

    public void searchMWOE() {
        if (this.nodeState == NodeState.TERMINATE) return;
        this.totalJoinMsg = this.neighbors.size() - this.treeNeighbors.size();
        checkComponentLeader(NodeState.SEARCH);
        int roundMsgNumber = this.treeEdges.size();
        while (this.getSearchRound() < N || (this.getSearchMsgNo() < roundMsgNumber * N)) {
            if (this.getSearchRound() < N && (this.getSearchMsgNo() == roundMsgNumber * this.getSearchRound())) {
                if (this.nodeState == NodeState.SEARCH) {
                    sendSearchMsg("SEARCH");
                    setNodeState(NodeState.IDLE);
                } else {
                    sendSearchMsg("EMPTY");
                }
                updateRound();
                updateSearchRound();
            }
        }
        updateRound();
    }

    public void initNextComponentLevel() {
        this.joinMsgNo = 0;
        this.round = 0;
        this.parent = null;
        this.nodeState = NodeState.IDLE;
        this.hasGlobalMWOE = false;
        this.MWOE = null;
        this.localMWOE = null;
        this.childrenMsgNo = 0;
    }

    public void checkComponentLeader(NodeState ns) {
        if (this.isLeader) {
            setNodeState(ns);
        }
    }

    public void selectLocalMWOE() {
        if (this.nodeState == NodeState.TERMINATE) return;
        initTestState();
        while (!this.edges.isEmpty() && MWOE == null) {
            if (getNodeState() == NodeState.TEST) {
                if (this.edges.isEmpty()) break;
                Edge mwoe = edges.peek();
                setNodeState(NodeState.IDLE);
                sendTestMsg(mwoe);
            }
        }
    }

    public void initTestState() {
        this.searchRound = 0;
        this.searchMsgNo = 0;
        this.nodeState = NodeState.TEST;
    }

    public void convergeLocalMWOE() {
        if (this.nodeState == NodeState.TERMINATE)
        initConvergeState();
        checkConverge();
        int roundMsgNumber = this.treeNeighbors.size() + (componentId == id ? 0 : -1);
        while (this.getConvergeRound() < N || (this.getConvergedMsgNo() < N * roundMsgNumber)) {
            if (this.getConvergeRound() < N && (this.getConvergedMsgNo() == this.getConvergeRound() * roundMsgNumber)) {
                if (this.nodeState == NodeState.CONVERGE) {
                    sendConvergeMsg("CONVERGE");
                    setNodeState(NodeState.IDLE);
                } else {
                    sendConvergeMsg("EMPTY");
                }
                updateRound();
                updateConvergeRound();
            }
        }
        updateRound();
    }

    public void initConvergeState() {
        this.nodeState = NodeState.IDLE;
    }

    public void checkConverge() {
        int requiredMsg = this.treeNeighbors.size() + (this.getComponentId() == this.id ? 0 : -1);
        Logger.Debug("Current required Msg is %s", requiredMsg);
        Logger.Debug("Current childrenMsgNo Msg is %s", this.childrenMsgNo);
        Logger.Debug("Current tree neighbors size is %s", this.treeNeighbors.size());
        if (this.childrenMsgNo == requiredMsg) {
            setNodeState(NodeState.CONVERGE);
        }
        Logger.Debug("Current state is %s", this.getNodeState());
    }

    public void sendMerge() {
        if (this.nodeState == NodeState.TERMINATE) return;
        initMergeState();
        Logger.Debug("Begin to merge, current MWOE is %s", this.MWOE);
        checkComponentLeader(NodeState.MERGE);
        if (checkTermination()) {
            setNodeState(NodeState.TERMINATE);
            sendTerminationMsg();
            return;
        }
        int roundMsgNumber = this.treeEdges.size();
        while (this.getMergeRound() < N || (this.getMergeMsgNo() < roundMsgNumber * N)) {
            if (this.nodeState == NodeState.TERMINATE) {
                break;
            }
            if (this.getMergeRound() < N && (this.getMergeMsgNo() == roundMsgNumber * this.getMergeRound())) {
                if (this.nodeState == NodeState.MERGE) {
                    sendMergeMsg("MERGE");
                    setNodeState(NodeState.IDLE);
                } else {
                    sendMergeMsg("EMPTY");
                }
                updateRound();
                updateMergeRound();
            }
        }
        updateRound();
        int res = compare(this.MWOE, this.localMWOE);
        if (this.localMWOE != null && res != 0) {
            this.edges.add(this.localMWOE);
        }
        
    }

    public void mergeMWOE() {
        if (this.nodeState == NodeState.TERMINATE) {
            return;
        }
        this.isLeader = false;
        setNodeState(NodeState.JOIN);
        sendJoinMsg();
        while (getNodeState() != NodeState.TERMINATE) {
            if (getNodeState() == NodeState.TERMINATE) {
                return;
            }
            if (getNodeState() != NodeState.JOIN) {
                break;
            }
        }
        updateTree();
        updateComponentLevel();
    }

    public void initMergeState() {
        this.convergeRound = 0;
        this.convergeMsgNo = 0;
        this.parent = null;
        this.nodeState = NodeState.IDLE;
        setNewComponentId(-1);
    }

    private boolean checkTermination() {
        if (componentId != id)
            return false;
        if (this.getMWOE() == null)
            return true;
        if (this.getMWOE().endpoint1 == id || this.getMWOE().endpoint2 == id) {
            this.hasGlobalMWOE = true;
        }
        return false;
    }

    public void updateTree() {
        initNextComponentLevel();
        for (Edge edge : newTreeEdges) {
            treeEdges.add(edge);
        }
        for (int id : newTreeNeighbors.keySet()) {
            if (treeNeighbors.containsKey(id)) continue;
            Node neighbor = neighbors.get(id);
            treeNeighbors.put(id, neighbor);
        }
        this.newTreeEdges = new LinkedList<>();
        this.newTreeNeighbors = new HashMap<>();
        Logger.Info("Before change, Current Component ID is %s", this.getComponentId());
        if (this.getId() == getNewComponentId()) {
            this.isLeader = true;
            setComponentId(this.getNewComponentId());
        } else {
            setComponentId(-1);
        }
        for (Edge e: this.edges) {
            Logger.Debug("Current edge in queue is: %s", e.toString());
        }
        Logger.Debug("Current is Leader value is %s", this.isLeader);
        Logger.Info("Component level %s complete", this.getComponentLevel());
        Logger.Info("Current Component ID is %s", this.getComponentId());
    }

    private synchronized void sendSearchMsg(String content) {
        Msg search = MsgFactory.searchMsg(this, content);
        broadcastMsg(search);
    }

    public void broadcastMsg(Msg msg) {
        for (Node node : treeNeighbors.values()) {
            msg.setToId(node.getId());
            msgService.sendMsg(msg);
        }
    }

    public void sendTestMsg(Edge edge) {
        int toId = edge.endpoint1 + edge.endpoint2 - this.id;
        Msg test = MsgFactory.testMsg(toId, componentId + "", this.componentLevel);
        msgService.sendMsg(test);
    }

    public void sendReplyMsg(int toId, String content) {
        Msg reply = MsgFactory.replyMsg(toId, content, this.componentLevel);
        msgService.sendMsg(reply);
    }

    public void sendConvergeMsg(String content) {
        if (parent == null)
            return;
        int toId = parent.id;
        if (content.equals("CONVERGE")) {
            if (this.MWOE == null) {
                content = "";
            } else {
                content = this.MWOE.toString();
            }
        }
        Msg converge = MsgFactory.convergeMsg(this, toId, content);
        msgService.sendMsg(converge);
    }

    private void sendMergeMsg(String content) {
        if (content.equals("MERGE")) {
            content = this.getMWOE().toString();
        }
        Msg merge = MsgFactory.mergeMsg(this, content);
        broadcastMsg(merge);
    }

    private void sendTerminationMsg() {
        Msg terminate = MsgFactory.terminateMsg(this);
        broadcastMsg(terminate);
    }

    private void sendJoinMsg() {
        Msg join;
        this.mergeRound = 0;
        this.mergeMsgNo = 0;
        for (int toId : neighbors.keySet()) {
            if (treeNeighbors.containsKey(toId))
                continue;
            if (hasGlobalMWOE && toId == MWOE.endpoint1 + MWOE.endpoint2 - id) {
                join = MsgFactory.joinMsg(this, toId, MWOE.toString());
                updateTreeNeighbors(toId, MWOE);
            } else {
                join = MsgFactory.joinMsg(this, toId, "EMPTY");
            }
            msgService.sendMsg(join);
            updateJoinMsgNo();
        }
        if (this.getJoinMsgNo() == 2 * this.totalJoinMsg) {
            setNodeState(NodeState.IDLE);
        }
    }
}
