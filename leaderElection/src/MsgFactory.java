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
}
