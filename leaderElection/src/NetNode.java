import java.io.*;
import java.util.HashMap;

public class NetNode {

    public static void main(String[] args) {
        try {

            String configPath = args.length > 0 ? args[0] : "../config.txt";
            String nodeId = args.length > 1 ? args[1] : "-1";

            Logger.setLocalNodeId(Integer.parseInt(nodeId));
            Logger.Info("Init node......");

            Node node = initNode(configPath, nodeId);

            node.startMsgService();

            StringBuilder tmp = new StringBuilder();
            for (int nId : node.getNeighbors().keySet()) {
                tmp.append(nId + "  ");
            }
            Logger.Info("Connecting Node: %s", tmp);

            Logger.Info("Send Msg or Press [d/D] to disconnect by NodeId.");
            testMode(node);
            electLeader(node);
            Logger.Debug("Leader election finished. The result is: %s", node.getPelegStatus());
            node.setRound(0);
            node.emptyMsgBuffer();

            Logger.Debug("Begin to create BFS tree.");
            buildTree(node);
            node.emptyTreeMsgBuffer();

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public static Node initNode(String configs, String nodeId) throws Exception {

        HashMap<String, String> nodes = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(configs))) {
            String line;
            int lineNum = 0;
            while ((line = br.readLine()) != null) {
                lineNum++;
                line = line.trim().toLowerCase();
                if (isValidLine(line)) {
                    String realLine = line.split("#")[0];
                    String[] t = realLine.split("\\s+");
                    if (t.length != 4) {
                        throw new Exception(String.format("Invalid configs at line %d", lineNum));
                    }
                    nodes.put(t[0], line);
                }
            }
        }

        String nodeInfo = nodes.get(nodeId);
        String[] t = nodeInfo.trim().split("\\s+");
        Node node = new Node(Integer.parseInt(t[0]), t[1], Integer.parseInt(t[2]));

        for (String key : t[3].trim().split(",")) {
            if (key.equals(nodeId)) continue;
            if (!nodes.containsKey(key)) {
                throw new Exception(String.format("Can not find neighbor [ %s ] in nodeList.", key));
            }
            String[] ss = nodes.get(key).trim().split("\\s+");
            node.addNeighbor(Integer.parseInt(ss[0]), ss[1].trim(), Integer.parseInt(ss[2]));
        }

        return node;
    }

    private static boolean isValidLine(String line) {
        return line.length() > 0 && Character.isDigit(line.charAt(0));
    }

    public static void testMode(Node node) {
        Runnable task = ()->{
            try {
                while (true) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    String msg = reader.readLine();
                    if (msg == null || msg.isEmpty()) {
                        continue;
                    }
                    if (msg.equalsIgnoreCase("D")) {
                        Logger.Info("Input nodeId:");
                        String ss = reader.readLine();
                        if (ss == null || ss.isEmpty()) continue;
                        node.disconnect(Integer.parseInt(ss));
                    } else {
                        node.sendTestMsg(msg);
                    }
                }
            } catch(Exception e) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                Logger.Error(sw.toString());
            }
        };
        new Thread(task).start();
    }

    public static void electLeader(Node node) {
        node.leaderElectInit();
        int roundMsgNumber = node.getNeighbors().size();
        while (node.getNodeState() == STATE.ELECTLEADER) {
            if (node.getRound() == 0 || node.getProcessedMsgNo() == roundMsgNumber * node.getRound()) {
                Logger.Debug("Round: %s, UID: %s, Dis: %s", node.getRound() + 1, node.getLargestUID(), node.getDistanceOfLargestUID());
                String msgContent = node.getLargestUID() + "," + node.getDistanceOfLargestUID();
                node.updateRound();
                node.sendElectMsg(msgContent);
            }
        }
    }

    public static void buildTree(Node node) {
        node.buildTreeInit();
        if(node.getPelegStatus() == LEADERELECTSTATUS.ISLEADER) {
            node.marked = true;
            node.setParent(node.getId());
        }
        while (node.getNodeState() == STATE.SEARCH) {
            if (node.getRound() == 0 || node.getProcessedMsgNo() == node.getNeighbors().size() * node.getRound()) {
                node.updateRound();
                if(node.marked){
                    node.sendSearchMsg("Search");
                    node.setNodeState(STATE.HOLD);
                }else{
                    node.sendSearchMsg("Null");
                }
            }
        }

        Logger.Debug( "Searching message is completed");
        while(node.getNodeState() != STATE.DONE) {
            if (node.getReplyMsgNo() == node.getNeighbors().size() && node.getChildren().size() == 0) {
                node.setNodeState(STATE.DONE);
            }
        }

        Logger.Debug( "ConvergeCast is completed");
        if(node.getParent() != node.getId()){
            int maxDegree = Math.max(node.getDegree(), node.getMaxDegree());
            node.replySearchMsg("" + maxDegree, node.getParent());
        }
        node.emptyTreeMsgBuffer();
        Logger.Debug("Node id is: %s", node.getId());
        Logger.Debug( "Node degree is: %s", node.getDegree());
        Logger.Debug( "Tree max degree is: %s", node.getMaxDegree());
        if (node.getPelegStatus() == LEADERELECTSTATUS.ISLEADER) {
            Logger.Debug( "Tree max degree is: %s", node.getMaxDegree());
            Logger.Debug("Node parent is null.");
        } else {
            Logger.Debug("Node parent is : %s", node.getParent());
        }
        if (node.getChildren().size() == 0) {
            Logger.Debug("Node children is null");
        } else {
            StringBuilder sb = new StringBuilder();
            for(int key: node.getChildren().keySet()){
                sb.append(key);
                sb.append(" ");
            }
            Logger.Debug("Node children is: %s", sb.toString());
        }
    }
}
