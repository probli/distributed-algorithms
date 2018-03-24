public class MsgFactory {
    private static int localNodeId = 0;
    public static void setLocalNodeId(int id) {
        localNodeId = id;
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

    public static Msg searchMsg(Node node, String content) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.SEARCH);
        msg.setContent(content);
        msg.setSrcId(node.getComponentId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound());
        return msg;
    }

    public static Msg testMsg(int toId, String content) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.TEST);
        msg.setContent(content);
        msg.setSrcId(localNodeId);
        msg.setFromId(localNodeId);
        msg.setToId(toId);
        msg.setRound(-1);
        return msg;
    }

    public static Msg replyMsg(int toId, String content) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.REPLY);
        msg.setContent(content);
        msg.setSrcId(localNodeId);
        msg.setFromId(localNodeId);
        msg.setToId(toId);
        msg.setRound(-1);
        return msg;
    }

    public static Msg convergeMsg(Node node, int toId, String content) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.CONVERGE);
        msg.setContent(content);
        msg.setSrcId(node.getId());
        msg.setFromId(node.getId());
        msg.setToId(toId);
        msg.setRound(node.getRound());
        return msg;
    }

    public static Msg mergeMsg(Node node, String content) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.MERGE);
        msg.setSrcId(node.getComponentId());
        msg.setFromId(node.getId());
        msg.setRound(node.getRound());
        return msg;
    }

    public static Msg joinMsg(int toId, String content) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.JOIN);
        msg.setContent(content);
        msg.setSrcId(localNodeId);
        msg.setFromId(localNodeId);
        msg.setToId(toId);
        msg.setRound(-1);
        return msg;
    }

    public static Msg terminateMsg(Node node) {
        Msg msg = new Msg();
        msg.setAction(MsgAction.TERMINATE);
        msg.setSrcId(node.getComponentId());
        msg.setFromId(node.getId());
        msg.setRound(-1);
        return msg;
    }
}
