import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MsgService {
    HashMap<Integer, MsgChannel> channels = new HashMap<>();

    Node nodeInfo;

    ServerSocket serverSocket;

    private List<MsgEventListener> listeners;

    public MsgService(Node node) {
        nodeInfo = node;
    }

    private boolean inChannelsReady = false;
    private boolean outChannelsReady = false;

    public boolean isInChannelsReady() {
        return inChannelsReady;
    }

    public boolean isOutChannelsReady() {
        return outChannelsReady;
    }

    public void startServer() throws Exception {
        String host = nodeInfo.getHost();
        int port = nodeInfo.getPort();
        serverSocket = new ServerSocket(port);

        for (Node node : nodeInfo.getNeighbors().values()) {
            MsgChannel ch = new MsgChannel(node.getId(), node.getHost(), node.getPort());
            channels.put(node.getId(), ch);
        }

        (new Thread() {
            @Override
            public void run() {
                try {
                    startInChannels();
                } catch (IOException e) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    Logger.Error(sw.toString());
                }
            }
        }).start();


    }

    private void startInChannels() throws IOException {

        int count = 0;
        while (count < channels.size()) {

            Socket socket = serverSocket.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String inputLine = in.readLine();
            //Logger.Debug(inputLine);
            Msg msg = new Msg(inputLine);

            if (msg.getAction().equals(MsgAction.CONNECT)) {
                MsgChannel ch = channels.getOrDefault(msg.getSrcId(), null);
                if (ch != null && !ch.hasInChannel()) {
                    ch.assignInChannel(socket, in);
                    count++;
                }
            }
        }

        inChannelsReady = true;
    }

    public void startOutChannels() throws IOException {

        int count = 0;
        while (count < channels.size()) {

            for (MsgChannel ch : channels.values()) {
                if (ch.hasOutChannel()) {
                    continue;
                }
                if (ch.connectOutChannel()) {
                    ch.setConnecting();
                    count++;
                }
            }
        }

        outChannelsReady = true;
    }

    public void registerEventListenser(MsgEventListener listener) {
        if (listeners == null) {
            listeners = new ArrayList<>();
        }
        listeners.add(listener);
    }

    public void listenToChannels() {
        for (MsgChannel ch : channels.values()) {
            if (!ch.isConnecting()) continue;
            (new Thread() {
                @Override
                public void run() {
                    try (
                            BufferedReader in = ch.getInChannel()
                    ) {
                        while (ch.isConnecting()) {
                            String inputLine;
                            while ((inputLine = ch.getInChannel().readLine()) != null) {
                                Msg msg = new Msg(inputLine);
                                Logger.Debug(String.format("[RECEIVED] %s | s: %d, f: %d, t: %d, r: %d, c: %s", msg.getAction(), msg.getSrcId(), msg.getFromId(), msg.getToId(), msg.getRound(), msg.getContent()));
                                onReceiveMsg(msg);
                            }
                        }
                        ch.shutdownInChannel();
                    } catch (IOException e) {
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        Logger.Error(sw.toString());
                    }
                }
            }).start();
        }
    }

    public void disconnect(int targetId) throws IOException {
        if (!channels.containsKey(targetId))
            return;

        channels.get(targetId).disconnect();
    }

    private void onReceiveMsg(Msg msg) {
        if (listeners.isEmpty()) return;

        for (MsgEventListener listener : listeners) {
            listener.onReceiveMsg(msg);
        }
    }

    public void sendMsg(Msg msg) {
        Logger.Debug(String.format("[SEND] %s | s: %d, f: %d, t: %d, r: %d, c: %s", msg.getAction(), msg.getSrcId(), msg.getFromId(), msg.getToId(), msg.getRound(), msg.getContent()));

        MsgChannel ch = channels.get(msg.getToId());
        ch.getOutChannel().println(msg.toString());
    }
}
