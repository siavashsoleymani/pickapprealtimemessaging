import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LiveStreamServer {
    private Map<Long, Socket> publisherSockets = new ConcurrentHashMap<>();
    private Map<Long, Socket> subscriberSockets = new ConcurrentHashMap<>();
    private static ExecutorService executorService = Executors.newCachedThreadPool();
    private ServerSocket server = null;
    private DataInputStream in = null;

    public LiveStreamServer(int port) {
        try {
            server = new ServerSocket(port);
            System.out.println("Server started");
            System.out.println("Waiting for a client ...");
            startAcceptingClients();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    private void startAcceptingClients() throws IOException {
        executorService.submit(() -> {
            while (true) {
                Socket socket = server.accept();
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                long id = Long.parseLong(in.readLine());
                System.out.println("Client accepted");
                if (id > 3000)
                    publisherSockets.put(id, socket);
                else
                    subscriberSockets.put(id, socket);
                CompletableFuture.runAsync(getRunnable(socket, id), executorService);
            }
        });
    }

    private Runnable getRunnable(Socket socket, long id) {
        if (id > 3000) {
            return () -> {
                while (true) {
                    try {
                        Thread.sleep(1);
                        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                        if (dataInputStream.available() > 0) {
                            Long recepientId;
                            Long timestamp;
                            String action;
                            try {
                                String data = dataInputStream.readLine();
                                if (data.startsWith("h"))
                                    continue;
                                String[] split = data.split(",");
                                recepientId = Long.valueOf(split[0]);
                                action = split[1];
                                timestamp = Long.valueOf(split[2]);
                            } catch (Exception e) {
                                System.out.println("Unknown message received");
                                continue;
                            }
                            Socket subscriberSocket = subscriberSockets.get(recepientId);
                            System.out.println(action + "," + timestamp);
                            if (subscriberSocket == null) {
                                System.out.println("Unknown receiver");
                                continue;
                            }
                            OutputStream outputStream = subscriberSocket.getOutputStream();
                            outputStream.write((action + "," + timestamp).getBytes());
                            outputStream.flush();
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
        } else return () -> {
            while (true) {
                try {
                    OutputStream outputStream = socket.getOutputStream();
                    outputStream.write("h\n".getBytes());
                    outputStream.flush();
                    Thread.sleep(5000);
                } catch (Exception e) {
                    System.out.println("one subscriber has been disconnected");
                    subscriberSockets.values().remove(socket);
                    break;
                }
            }
        };
    }

    public static void main(String args[]) {
        new LiveStreamServer(Integer.parseInt(args[0]));
    }
}
