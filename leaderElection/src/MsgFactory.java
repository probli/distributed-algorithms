public class MsgFactory {
    private static int localNodeId = 0;

    public static void setLocalNodeId(int id) {
        localNodeId = id;
    }

    public static Msg testMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.TEST);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound());
        return msg;
    }

    public static Msg connectMsg(int toId) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.CONNECT);
        msg.setSrcId(localNodeId);
        msg.setFromId(localNodeId);
        msg.setToId(toId);
        msg.setRound(-1);
        return msg;
    }

    public static Msg disconnectMsg(int toId) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.DISCONNECT);
        msg.setSrcId(localNodeId);
        msg.setFromId(localNodeId);
        msg.setToId(toId);
        msg.setRound(-1);
        return msg;
    }

    public static Msg electMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.ELECTLEADER);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound() + 1);
        msg.setContent(node.getLargestUID() + "," + node.getDistanceOfLargestUID());
        return msg;
    }

    public static Msg replyMsg(Node node, String s, int to) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.REPLY);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setToId(to);
        msg.setRound(-1);
        msg.setContent(s);
        return msg;
    }

    public static Msg buildMsg(Node node, String s) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.BUILD);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound() + 1);
        msg.setContent(s);
        return msg;
    }

    public static Msg degreeMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.DEGREE);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(-1);
        return msg;
    }

    public static Msg endMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.END);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(-1);
        return msg;
    }
}
