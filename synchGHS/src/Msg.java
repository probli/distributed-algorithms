public class Msg {
    private String action;
    private int srcId;
    private int fromId;
    private int toId;
    private int round;
    private String content = "";

    public Msg(String str) {
        String[] token = str.split("\\|");
        this.action = token[0];
        this.srcId = Integer.parseInt(token[1]);
        this.fromId = Integer.parseInt(token[2]);
        this.toId = Integer.parseInt(token[3]);
        this.round = Integer.parseInt(token[4]);
        this.content = token.length < 6 ? "" : token[5];
    }

    public Msg() {
    }

    public String getAction() {
        return action;
    }

    public void setAction(String a) {
        action = a;
    }

    public int getSrcId() {
        return srcId;
    }

    public void setSrcId(int v) {
        srcId = v;
    }

    public int getFromId() {
        return fromId;
    }

    public void setFromId(int v) {
        fromId = v;
    }

    public int getToId() {
        return toId;
    }

    public void setToId(int v) {
        toId = v;
    }

    public int getRound() {
        return round;
    }

    public void setRound(int v) {
        round = v;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String c) {
        content = c;
    }

    @Override
    public String toString() {
        return String.format("%s|%d|%d|%d|%d|%s", this.action, this.srcId, this.fromId, this.toId, this.round, this.content);
    }


    public String printFormat() {
        return String.format("%s | s: %d, f: %d, t: %d, r: %d, c: %s", this.action, this.srcId, this.fromId, this.toId, this.round, this.content);
    }
}
