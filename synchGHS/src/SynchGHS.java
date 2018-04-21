import java.io.*;
import java.util.*;

public class SynchGHS {

    public static void main(String[] args) {
        try {
            String configPath = args.length > 0 ? args[0] : "../config.txt";
            String nodeId = args.length > 1 ? args[1] : "-1";
            boolean debugMode = args.length > 2 ? args[2].equals("-d") : false;
            Logger.setDebugMode(debugMode);
            Logger.setLocalNodeId(Integer.parseInt(nodeId));
            Logger.Info("Init node......");

            Node node = initNode(configPath, nodeId);
            node.startMsgService();

            StringBuilder tmp = new StringBuilder();
            for (int nId : node.getNeighbors().keySet()) {
                tmp.append(nId + "  ");
            }
            Logger.Info("Connecting Node: %s", tmp);
            testMode(node);
            buildMST(node);
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
                    String readLine = line.split("#")[0].trim();
                    String[] t = readLine.split("\\s+");
                    if (nodeNum == -1) {
                        nodeNum = Integer.parseInt(readLine);
                        lineNum = 0;
                    } else if (lineNum <= nodeNum) {
                        if (t.length != 3) {
                            throw new Exception(String.format("Invalid configs at line %d", lineNum));
                        }
                        nodes.put(t[0], readLine);

                        if (t[0].equals(nodeId)) {
                            node = new Node(Integer.parseInt(t[0]), t[1], Integer.parseInt(t[2]));
                        }
                    } else if (lineNum > nodeNum) {
                        if (!t[0].startsWith("(") || !t[0].endsWith(")")) {
                            throw new Exception(String.format("Invalid edge format: %s", readLine));
                        }
                        String[] points = t[0].substring(1, t[0].length() - 1).split(",");
                        String id1 = points[0].trim();
                        String id2 = points[1].trim();
                        if (!id1.equals(nodeId) && !id2.equals(nodeId)) {
                            continue;
                        }
                        if (id1.equals(id2)) {
                            throw new Exception(String.format("Invalid edge Id: %s", readLine));
                        }
                        String nbIdStr = id1.equals(nodeId) ? id2 : id1;
                        String[] nbInfo = nodes.get(nbIdStr).split("\\s+");
                        int nId = Integer.parseInt(nbInfo[0]);
                        String nHost = nbInfo[1].trim();
                        int nPort = Integer.parseInt(nbInfo[2]);
                        int weight = Integer.parseInt(t[1].trim());
                        node.setN(nodeNum);
                        node.addNeighbor(nId, nHost, nPort, weight);
                    }
                }
            }
        }
        return node;
    }

    private static boolean isValidLine(String line) {
        return line.length() > 0 && (Character.isDigit(line.charAt(0)) || line.charAt(0) == '(');
    }

    public static void buildMST(Node node) {
        node.initBuildMST();

        while (node.getNodeState() != NodeState.TERMINATE) {
            node.updateComponentLevel();
            node.searchMWOE();
            node.selectLocalMWOE();
            node.convergeLocalMWOE();
            node.sendMerge();
            if (node.getNodeState() == NodeState.TERMINATE) break;
            node.mergeMWOE();
        }

        Logger.Info("MST Created!");
        Logger.Info("[RESULT] Final component ID is %s", node.getComponentId());
        printInfo(node);
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
                    } else if (msg.equalsIgnoreCase("BUFFER")){
                        node.printMsgInBuffer();
                    } else if (msg.equalsIgnoreCase("INFO")){
                        Logger.Info("MsfNo: %s, R: %s, CLevel:  %s", node.getProcessedMsgNo(), node.getRound(), node.getComponentLevel());
                    } else if (msg.equalsIgnoreCase("RESULT")){
                        printInfo(node);
                    } else {
                        Logger.Info("Not Supported");
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

    private static void printInfo(Node node) {
        StringBuilder sb = new StringBuilder();
        for (Edge e: node.getTreeEdges()) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            int end = e.endpoint1 == node.getId()? e.endpoint2 : e.endpoint1;
            sb.append(end);
            sb.append(" - (" + e.weight + ")");
        }
        Logger.Info("[RESULT] Tree Edges %s : {%s}", node.getId(), sb.toString());
    }
}
