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

            // only enable when debugging connections
            // testMode(node);

            Logger.Info("Begin to elect leader.");
            electLeader(node);
            Logger.Info("Leader election finished. The result is: %s", node.getElectState());

            Logger.Info("Begin to create BFS tree.");
            buildTree(node);
            Logger.Info("BFS tree building finished.");
            
            Logger.Info("P: %s ---> %s", node.getParent() == node.getId() ? " null" : node.getParent(), node.getId());

            StringBuilder sb = new StringBuilder();
            for (int key : node.getChildren().keySet()) {
                sb.append(key);
                sb.append(", ");
            }

            Logger.Info("Node %s : {%s}", node.getId(), sb.toString());

            if (node.getElectState() == ElectState.ISLEADER) {
                Logger.Info("----------------------------------  Tree max degree is: %s", node.getMaxDegree());
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            Logger.Error(sw.toString());
        }
    }

    public static Node initNode(String configs, String nodeId) throws Exception {

        Node node = null;
        HashMap<String, String> nodes = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(configs))) {
            String line;
            int lineNum = 0;
            int nodeNum = -1;
            while ((line = br.readLine()) != null) {

                line = line.trim().toLowerCase();
                if (isValidLine(line)) {
                    lineNum++;
                    String readLine = line.split("#")[0];
                    String[] t = readLine.split("\\s+");
                    if (nodeNum == -1) {
                        nodeNum = Integer.parseInt(readLine);
                        lineNum = 0;
                    } else if (lineNum <= nodeNum) {
                        if (t.length != 3) {
                            throw new Exception(String.format("Invalid configs at line %d", lineNum));
                        }
                        nodes.put(t[0], line);
                    } else if (lineNum > nodeNum) {
                        if(!t[0].equals(String.valueOf(nodeId))) {
                            continue;
                        }
                        String nodeInfo = nodes.get(nodeId);
                        String[] s1 = nodeInfo.trim().split("\\s+");
                        node = new Node(Integer.parseInt(s1[0]), s1[1], Integer.parseInt(s1[2]));
                        for (int i = 1; i < t.length; i++) {
                            if (t[i].equals(nodeId)) continue;
                            if (!nodes.containsKey(t[i])) {
                                throw new Exception(String.format("Can not find neighbor [ %s ] in nodeList.", t[i]));
                            }
                            String[] s2 = nodes.get(t[i]).trim().split("\\s+");
                            node.addNeighbor(Integer.parseInt(s2[0]), s2[1].trim(), Integer.parseInt(s2[2]));
                        }
                        break;
                    }
                }
            }
        }

        return node;
    }

    private static boolean isValidLine(String line) {
        return line.length() > 0 && Character.isDigit(line.charAt(0));
    }

    public static void testMode(Node node) {

        Logger.Info("Send Msg or Press [d/D] to disconnect by NodeId.");

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
                    } else if (msg.equalsIgnoreCase("P")){
                        node.printMsgInBuffer();
                    } else if (msg.equalsIgnoreCase("R")){
                        Logger.Info("MsfNo: %s, R: %s, Size: %s", node.getProcessedMsgNoBuild(), node.getRound(), node.getNeighbors().size());
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
        while (node.getNodeState() == NodeState.ELECT) {
            if (node.getProcessedMsgNoElect() == roundMsgNumber * node.getRound()) {
                node.sendElectMsg();
                node.updateRound();
                Logger.Info("Round: %s, UID: %s, Dis: %s", node.getRound(), node.getLargestUID(), node.getDistanceOfLargestUID());
            }
        }
    }

    public static void buildTree(Node node) {
        node.buildTreeInit();
        node.markLeader();

        int roundMsgNumber = node.getNeighbors().size();
        while (node.getBuildTreeState() != BuildTreeState.DONE) {
            if (node.getProcessedMsgNoBuild() == roundMsgNumber * node.getRound()) {
                if (node.getBuildTreeState() == BuildTreeState.MARKED) {
                    node.sendSearchMsg();
                    node.setBuildTreeState(BuildTreeState.WAITING);
                } else {
                    node.sendEmptyMsg();
                }
                node.updateRound();
            }
        }
    }
}
