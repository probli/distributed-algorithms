import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import sun.rmi.runtime.Log;

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
    private List<Edge> treeEdges = new LinkedList<>();
    private HashMap<Integer, Node> treeNeighbors = new HashMap<>();
    private List<Edge> newTreeEdges = new LinkedList<>();
    private HashMap<Integer, Node> newTreeNeighbors = new HashMap<>();
    private int totalJoinMsg;
    private Edge MWOE;
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
                                //Logger.Debug("This message is from buffer %s", m);
                                //Logger.Debug("Remove BUFFER message is %s", m);
                                processMsg(m);
                                bufferedMsg.remove(m);
                                //Logger.Debug("BUFFER size is %s", this.bufferedMsg.size());
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
            //Logger.Debug("Add BUFFER message is %s", msg);
        }
    }

    private void processMsg(Msg msg) {
        try {
            if (msg.getComponentLevel() > this.getComponentLevel()
                    || (msg.getRound() > this.getRound() && msg.getRound() != -1)) {
                //Logger.Debug("This round number: %s", this.getRound());
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
        return searchMsgNo;
    }

    private synchronized int getConvergedMsgNo() {
        return convergeMsgNo;
    }

    private synchronized int getMergeMsgNo() {
        return mergeMsgNo;
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
        //Logger.Debug("Handle mwoe is %s", mwoe);
        if (content.equals("ACCEPT")) {
            setMWOE(mwoe);
            // Logger.Debug("This MWOE is %s", this.MWOE);
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
        //Logger.Debug("Curent MWOE is %s", MWOE);
        //Logger.Debug("Curent mwoe is %s", mwoe);
        if (res > 0) {
            setMWOE(mwoe);
            //Logger.Debug("After Curent MWOE is %s", MWOE);
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
        Logger.Debug("Current total join message size is %s", this.totalJoinMsg);
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
        //Logger.Debug("%s", res);
        Logger.Debug("%s", hasGlobalMWOE);
        if (hasGlobalMWOE && (res == 0 || MWOE == null)) {
            setNewComponentId(Math.max(ep1, ep2));
            Logger.Debug("new componet id:%s", getNewComponentId());
        } else {
            //Logger.Debug("Receive message to update tree neighbor");
            updateTreeNeighbors(fromId, mwoe);
        }
    }

    public synchronized void updateTreeNeighbors(int id, Edge edge) {
        //Logger.Debug("update new tree neighbors");
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
        totalJoinMsg = this.neighbors.size() - this.treeNeighbors.size();
        checkComponentLeader(NodeState.SEARCH);
        int roundMsgNumber = this.treeEdges.size();
        while (this.getSearchRound() < N || (this.getSearchMsgNo() < roundMsgNumber * N)) {
            if (this.getSearchRound() < N && (this.getSearchMsgNo() >= roundMsgNumber * this.getSearchRound())) {
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
    }

    public void initNextComponentLevel() {
        this.joinMsgNo = 0;
        this.round = 0;
        this.parent = null;
        this.nodeState = NodeState.IDLE;
        this.hasGlobalMWOE = false;
        this.MWOE = null;
    }

    public void checkComponentLeader(NodeState ns) {
        if (this.componentId == this.id) {
            setNodeState(ns);
        }
    }

    public void selectLocalMWOE() {
        initTestState();
        while (MWOE == null) {
            if (getNodeState() == NodeState.TEST) {
                if (this.edges.isEmpty()) {
                    break;
                } else {
                    Edge mwoe = edges.peek();
                    sendTestMsg(mwoe);
                    setNodeState(NodeState.IDLE);
                }
            }
        }
    }

    public void initTestState() {
        this.searchRound = 0;
        this.searchMsgNo = 0;
        this.nodeState = NodeState.TEST;
    }

    public void convergeLocalMWOE() {
        this.updateRound();
        initConvergeState();
        checkConverge();
        int roundMsgNumber = this.treeNeighbors.size() + (componentId == id ? 0 : -1);
        while (this.getConvergeRound() < N || (this.getConvergedMsgNo() < N * roundMsgNumber)) {
            if (this.getConvergeRound() < N && (this.getConvergedMsgNo() >= this.getConvergeRound() * roundMsgNumber)) {
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
    }

    public void initConvergeState() {
        this.childrenMsgNo = 0;
        this.nodeState = NodeState.IDLE;
    }

    public void checkConverge() {
        int requiredMsg = this.treeNeighbors.size() + (componentId == id ? 0 : -1);
        if (this.childrenMsgNo == requiredMsg) {
            setNodeState(NodeState.CONVERGE);
        }
    }

    public void sendMerge() {
        this.updateRound();
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
            if (this.getMergeRound() < N && (this.getMergeMsgNo() >= roundMsgNumber * this.getMergeRound())) {
                if (this.nodeState == NodeState.MERGE) {
                    sendMergeMsg("MERGE");
                    setNodeState(NodeState.IDLE);
                    // String[] edge = this.getMWOE().toString().split(",");
                    // int ep1 = Integer.parseInt(edge[0]);
                    // int ep2 = Integer.parseInt(edge[1]);
                    // this.setNewComponentId(Math.max(ep1, ep2));
                } else {
                    sendMergeMsg("EMPTY");
                }
                updateRound();
                updateMergeRound();
            }
        }
    }

    public void mergeMWOE() {
        if (this.nodeState == NodeState.TERMINATE) {
            return;
        }
        //Logger.Debug("Begin to join ");
        //Logger.Debug("Processed Msg set to zero");
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
        // Logger.Debug("Component leader is %s", getComponentId());
        // Logger.Debug(" This join round complete ");
        // Logger.Debug("neighbor size is %s ", getNeighbors().size());
        // Logger.Debug("tree neighbor size is %s", getTreeNeighbors().size());
        updateComponentLevel();
    }

    public void initMergeState() {
        this.convergeRound = 0;
        this.convergeMsgNo = 0;
        this.parent = null;
        this.nodeState = NodeState.IDLE;
        // setNewComponentId(this.getComponentId());
        setNewComponentId(-1);
    }

    private boolean checkTermination() {
        if (componentId != id)
            return false;
        if (MWOE == null)
            return true;
        if (MWOE.endpoint1 == id || MWOE.endpoint2 == id) {
            this.hasGlobalMWOE = true;
        }
        return false;
    }

    public void updateTree() {
        initNextComponentLevel();
        //Logger.Debug("this new component is %s", this.getNewComponentId());
        //Logger.Debug("this component is %s", this.getComponentId());
        if (this.getNewComponentId() != -1) {
            this.setComponentId(this.getNewComponentId());
        }
        //Logger.Debug("After merge this component is %s", this.getComponentId());
        for (Edge edge : newTreeEdges) {
            //Logger.Debug("Edge is %s", edge);
            treeEdges.add(edge);
        }
        for (int id : newTreeNeighbors.keySet()) {
            Node neighbor = neighbors.get(id);
            treeNeighbors.put(id, neighbor);
        }
        this.newTreeEdges = new LinkedList<>();
        this.newTreeNeighbors = new HashMap<>();
        Logger.Debug("Component level %s complete", this.getComponentLevel());
        Logger.Debug("Current Component ID is %s", this.getComponentId());
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
        Logger.Debug("Current total join message size is %s", this.totalJoinMsg);
        for (int toId : neighbors.keySet()) {
            if (treeNeighbors.containsKey(toId))
                continue;
            if (hasGlobalMWOE && toId == MWOE.endpoint1 + MWOE.endpoint2 - id) {
                join = MsgFactory.joinMsg(this, toId, MWOE.toString());
                setNewComponentId(Math.max(this.getNewComponentId(), toId));
                Logger.Debug("Current new componentId is %s", getNewComponentId());
                //Logger.Debug("Send message to update tree neighbor");
                updateTreeNeighbors(toId, MWOE);
            } else {
                join = MsgFactory.joinMsg(this, toId, "EMPTY");
            }
            msgService.sendMsg(join);
            updateJoinMsgNo();
            if (this.getJoinMsgNo() == 2 * this.totalJoinMsg) {
                setNodeState(NodeState.IDLE);
            }
        }
    }
}
