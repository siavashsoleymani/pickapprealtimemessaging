import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Publisher {
    private static Socket socket = null;

    public Publisher(String address, int port) throws IOException {
        connect(address, port);
        clientIntroduction();
        startHeartBeat(address, port);
        getFromInput();
    }

    private void getFromInput() {
        while (true) {
            Scanner sc = new Scanner(System.in);
            String s = sc.nextLine();
            sendAction(s, 2001l);
        }
    }

    public static void sendAction(String action, Long recipientId) {
        try {
            OutputStream outputStream = socket.getOutputStream();
            String message = recipientId + "," + action + "," + System.currentTimeMillis() + "\n";
            System.out.println(message);
            outputStream.write(message.getBytes());
            outputStream.flush();
        } catch (IOException e) {
            System.out.println("couldn't send action message");
        }
    }

    private void clientIntroduction() throws IOException {
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
        outputStream.write("3001\n".getBytes());
        outputStream.flush();
    }

    private void startHeartBeat(String address, int port) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            while (true) {
                try {
                    OutputStream outputStream = socket.getOutputStream();
                    outputStream.write("h\n".getBytes());
                    outputStream.flush();
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                    connect(address, port);
                    clientIntroduction();
                }
            }
        });
    }

    public static void main(String args[]) throws IOException {
        new Publisher("127.0.0.1", 5000);
    }

    private static void connect(String address, int port) {
        try {
            socket = new Socket(address, port);
        } catch (IOException e) {
            System.out.println("cant connect to server :" + e.getMessage());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
            connect(address, port);
        }
    }
} 