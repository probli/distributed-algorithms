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
        msg.setRound(0);
        return msg;
    }

    public static Msg disconnectMsg(int toId) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.DISCONNECT);
        msg.setSrcId(localNodeId);
        msg.setFromId(localNodeId);
        msg.setToId(toId);
        msg.setRound(0);
        return msg;
    }

    public static Msg electMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.ELECTLEADER);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound());
        msg.setContent(node.getLargestUID() + "," + node.getDistanceOfLargestUID());
        return msg;
    }

    public static Msg searchMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.SEARCH);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound());
        return msg;
    }

    public static Msg acceptMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.ACCEPT);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(0);
        return msg;
    }

    public static Msg rejectMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.REJECT);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(0);
        return msg;
    }

    public static Msg emptyMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.EMPTY);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound());
        return msg;
    }

    public static Msg degreeMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.DEGREE);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setRound(0);
        return msg;
    }
}
