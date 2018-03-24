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
    private static ConcurrentLinkedQueue<Msg> bufferedMsg = new ConcurrentLinkedQueue<>();

    private NodeState nodeState;
    private int round;
    private int componentId;
    private List<Edge> treeEdges;
    private HashMap<Integer, Node> treeNeighbors;
    private List<Edge> newTreeEdges;
    private HashMap<Integer, Node> newTreeNeighbors;
    private Edge MWOE;
    private int processedMsgNo;
    private int childrenMsgNo;
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
                            if (m.getRound() == getRound() || m.getRound() == -1) {
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
            } else if (msg.getAction().equals(MsgAction.SEARCH)) {
                String content = msg.getContent();
                if (content.equals("SEARCH")) {
                    int fromId = msg.getFromId();
                    int srcId = msg.getSrcId();
                    processSearchMsg(fromId, srcId);
                }

                updateProcessedMsgNo();
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

                updateProcessedMsgNo();
            } else if (msg.getAction().equals(MsgAction.MERGE)) {
                String content = msg.getContent();
                if (!content.equals("EMPTY")) {
                    int fromId = msg.getFromId();
                    int srcId = msg.getSrcId();
                    processMergeMsg(fromId, srcId, content);
                }

                updateProcessedMsgNo();
            } else if (msg.getAction().equals(MsgAction.JOIN)) {
                if (nodeState != NodeState.JOIN) {
                    addMsgToBuffer(msg);
                    return;
                }

                int fromId = msg.getFromId();
                String content = msg.getContent();
                processJoinMsg(fromId, content);
            } else if (msg.getAction().equals(MsgAction.TERMINATE)) {
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

    private synchronized void updateProcessedMsgNo() {
        this.processedMsgNo = this.processedMsgNo + 1;
    }

    private synchronized void updateChildrenMsgNo() {
        this.childrenMsgNo = this.childrenMsgNo + 1;
    }

    private synchronized void processTestMsg(int fromId, int receivedId) {
        if (receivedId != componentId) {
            sendReplyMsg(fromId, "ACCEPT");
        } else {
            sendReplyMsg(fromId, "REJECT");
        }
    }

    private synchronized void processReplyMsg(String content) {
        Edge mwoe = edges.poll();
        if (content.equals("ACCEPT")) {
            this.MWOE = mwoe;
        } else if (content.equals("REJECT")) {
            setNodeState(NodeState.TEST);
        }
    }

    private synchronized void processConvergeMsg(String content) {
        if (!content.equals("null")) {
            String[] edge = content.split(",");
            int ep1 = Integer.parseInt(edge[0]);
            int ep2 = Integer.parseInt(edge[1]);
            int weight = Integer.parseInt(edge[2]);

            Edge mwoe = new Edge(ep1, ep2, weight);
            int res = compare(MWOE, mwoe);
            if (res > 0) MWOE = mwoe;
        }
    }

    private synchronized void processMergeMsg(int fromId, int srcId, String content) {
        if (parent == null && srcId != id) {
            String[] edge = content.split(",");
            int ep1 = Integer.parseInt(edge[0]);
            int ep2 = Integer.parseInt(edge[1]);
            int weight = Integer.parseInt(edge[2]);

            Edge mwoe = new Edge(ep1, ep2, weight);
            MWOE = mwoe;
            if (ep1 == id || ep2 == id) {
                this.hasGlobalMWOE = true;
            }
            
            setParent(fromId);
            setNodeState(NodeState.MERGE);
        }
    }

    private synchronized void processJoinMsg(int fromId, String content) {
        String[] edge = content.split(",");
        int ep1 = Integer.parseInt(edge[0]);
        int ep2 = Integer.parseInt(edge[1]);
        int weight = Integer.parseInt(edge[2]);
        Edge mwoe = new Edge(ep1, ep2, weight);

        int res = compare(MWOE, mwoe);
        Logger.Debug("%s", res);
        Logger.Debug("%s", hasGlobalMWOE);
        if (hasGlobalMWOE && res == 0) {
            int newComponentId = Math.max(ep1, ep2);
            setComponentId(newComponentId);
            Logger.Debug("new componet id:%s", newComponentId);
        } else {
            updateTreeNeighbors(fromId, mwoe);
        }
    }

    public synchronized void updateTreeNeighbors(int id, Edge edge) {
        Logger.Debug("tada");
        Node newTreeNeighbor = neighbors.get(id);
        newTreeNeighbors.put(id, newTreeNeighbor);
        newTreeEdges.add(edge);
        Logger.Debug("Size: %s", newTreeEdges.size());
    }

    private void transferToChildren(Msg msg) {
        for (Node node : treeNeighbors.values()) {
            if (msg.getFromId() != node.getId()) {
                msg.setToId(node.getId());
                msgService.sendMsg(msg);
            }
        }
    }

    public int compare(Edge e1, Edge e2) {
        if (e1 == null) return 1;
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
        // this.edges.put(weight, id);
        this.edges.offer(new Edge(this.id, id, weight));
    }

    public int getRound() {
        return round;
    }

    public void updateRound() {
        this.round =  this.round + 1;
    }

    public void setRound(int r) {
        this.round = r;
    }

    public synchronized NodeState getNodeState() {
        return this.nodeState;
    }

    public synchronized void setNodeState(NodeState s) {
        Logger.Info("Round %s : %s ----> %s", getRound(),this.nodeState, s);
        this.nodeState = s;
    }

    public int getComponentId() {
        return this.componentId;
    }

    public void setComponentId(int c) {
        this.componentId = c;
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

    public Edge getMWOE() {
        return MWOE;
    }

    public void setMWOE(Edge e) {
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
        this.treeNeighbors = new HashMap<>();
        this.treeEdges = new LinkedList<>();
    }

    public void searchMWOE() {
        initSearchState();
        checkComponentLeader(NodeState.SEARCH);

        int roundMsgNumber = this.getTreeEdges().size();
        while (this.round < N) {
            if (this.processedMsgNo == roundMsgNumber * this.round) {
                if (this.nodeState == NodeState.SEARCH) {
                    sendSearchMsg("SEARCH");
                    setNodeState(NodeState.IDLE);
                } else {
                    sendSearchMsg("EMPTY");
                }

                updateRound();
            }
        }
    }

    public void initSearchState() {
        this.round = 0;
        this.processedMsgNo = 0;
        this.parent = null;
        this.nodeState = NodeState.IDLE;
        this.hasGlobalMWOE = false;
        this.newTreeEdges = new LinkedList<>();
        this.newTreeNeighbors = new HashMap<>();
    }

    public void checkComponentLeader(NodeState ns) {
        if (this.componentId == this.id) {
            setNodeState(ns);
        }
    }

    public void selectLocalMWOE() {
        initTestState();

        // STRANGE!
        while (!this.edges.isEmpty() && MWOE == null) {
            if (getNodeState() == NodeState.TEST) {
                Edge mwoe = edges.peek();
                setNodeState(NodeState.IDLE);
                sendTestMsg(mwoe);
            }
        }
    }

    public void initTestState() {
        this.round = 0;
        this.MWOE = null;
        this.nodeState = NodeState.TEST;
    }

    public void convergeLocalMWOE() {
        initConvergeState();
        checkConverge();

        int roundMsgNumber = this.treeNeighbors.size() + (componentId == id ? 0: -1);
        while (this.round < N) {
            if (this.processedMsgNo == this.round * roundMsgNumber) {
                if (this.nodeState == NodeState.CONVERGE) {
                    sendConvergeMsg("CONVERGE");
                    setNodeState(NodeState.IDLE);
                } else {
                    sendConvergeMsg("EMPTY");
                }
            }

            updateRound();
        }
    }

    public void initConvergeState() {
        this.round = 0;
        this.processedMsgNo = 0;
        this.childrenMsgNo = 0;
        this.nodeState = NodeState.IDLE;
    }

    public void checkConverge() {
        int requiredMsg = this.treeNeighbors.size() + (componentId == id ? 0: -1);
        if (this.childrenMsgNo == requiredMsg) {
            setNodeState(NodeState.CONVERGE);
        }
    }

    public void mergeMWOE() {
        initMergeState();
        checkComponentLeader(NodeState.MERGE);
        if (checkTermination()) {
            // sendTerminationMsg()
            return;
        }

        int roundMsgNumber = this.treeEdges.size();
        while (this.round < N) {
            if (this.processedMsgNo == roundMsgNumber * this.round) {
                if (this.nodeState == NodeState.MERGE) {
                    sendMergeMsg("MERGE");
                    setNodeState(NodeState.IDLE);
                } else {
                    sendMergeMsg("EMPTY");
                }

                updateRound();
            }
        }

        setNodeState(NodeState.JOIN);

        if (hasGlobalMWOE) {
            join();
        }
    }

    public void initMergeState() {
        this.round = 0;
        this.processedMsgNo = 0;
        this.parent = null;
        this.nodeState = NodeState.IDLE;
        
    }

    private boolean checkTermination() {
        if (componentId != id) return false;
        if (MWOE == null) return true;
        if (MWOE.endpoint1 == id || MWOE.endpoint2 == id) {
            this.hasGlobalMWOE = true;
        }

        return false;
    }

    private void join() {
        int toId = MWOE.endpoint1 + MWOE.endpoint2 - id;
        Msg join = MsgFactory.joinMsg(toId, MWOE.toString());
        msgService.sendMsg(join);
        updateTreeNeighbors(toId, MWOE);
    }

    public void updateTree() {
        for (Edge edge : newTreeEdges) {
            treeEdges.add(edge);
        }
        for (int id : newTreeNeighbors.keySet()) {
            Node neighbor = neighbors.get(id);
            treeNeighbors.put(id, neighbor);
        }
    }

    private void sendSearchMsg(String content) {
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
        int toId = edge.endpoint1 + edge.endpoint2 - id;
        Msg test = MsgFactory.testMsg(toId, componentId + "");
        msgService.sendMsg(test);
    }

    public void sendReplyMsg(int toId, String content) {
        Msg reply = MsgFactory.replyMsg(toId, content);
        msgService.sendMsg(reply);
    }

    public void sendConvergeMsg(String content) {
        if (parent == null) return;
        int toId = parent.id;
        if (content.equals("CONVERGE")) content = MWOE.toString();
        Msg converge = MsgFactory.convergeMsg(this, toId, content);
        msgService.sendMsg(converge);
    }

    private void sendMergeMsg(String content) {
        if (content.equals("MERGE")) content = MWOE.toString();
        Msg merge = MsgFactory.mergeMsg(this, content);
        broadcastMsg(merge);
    }

    private void sendTerminationMsg() {
        Msg terminate = MsgFactory.terminateMsg(this);
        broadcastMsg(terminate);
    }
}
