package pl.mikolajbiel.scylla;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final Socket socket;
    private final AtomicInteger sequence;
    private static final ReentrantLock outputStreamLock = new ReentrantLock();
    private final ConcurrentHashMap<Integer, byte[]> responseMap = new ConcurrentHashMap<>();
    private Thread responseListener;

    private static Session session;

    private Session(SocketFactory socketFactory, InetSocketAddress address) throws IOException {
        this.sequence = new AtomicInteger();
        this.socket = socketFactory.createSocket(address.getHostName(), address.getPort());
        this.inputStream = socket.getInputStream();
        this.outputStream = socket.getOutputStream();
    }

    public static Session connect(InetSocketAddress address) {
        return connect(SocketFactory.getDefault(), address);
    }

    public static Session connect(SocketFactory socketFactory, InetSocketAddress address) {
        if (session != null) {
            session.disconnect();
        }

        try {
            session = new Session(socketFactory, address);
            session.startup();
        } catch (IOException e) {
            logger.error("Error on startup", e);
            throw new RuntimeException(e);
        }

        return session;
    }

    private void listenForResponses() {
        responseListener = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] response = new byte[13];
                    int sizeOfRead = inputStream.read(response);
                    short responseSequence = ByteBuffer.wrap(response).getShort(2);
                    responseMap.put((int) responseSequence, response);
                } catch (IOException e) {
                    logger.error("Error on Stream operation... ", e);
                    throw new RuntimeException(e);
                }
            }
        });
        responseListener.setDaemon(true);
        responseListener.start();
    }

    private void disconnect() {
        try {
            inputStream.close();
            outputStream.close();
            socket.close();
            responseListener.interrupt();
        } catch (IOException e) {
            logger.error("Error while closing the connection", e);
            throw new RuntimeException(e);
        }
    }

    private void startup() throws IOException {
        byte[] startupMessage = CQLMapper.createStartupMessage();
        outputStreamLock.lock();
        outputStream.write(startupMessage, 0, startupMessage.length);
        outputStream.flush();
        outputStreamLock.unlock();

        byte[] response = new byte[32];
        int sizeOfRead = inputStream.read(response);
        logger.info("Startup response: {}", CQLMapper.convertToHexString(response, sizeOfRead));

        listenForResponses();
    }

    public void execute(String statement) {
        int requestSequence = this.sequence.getAndIncrement();
        byte[] queryMessage = CQLMapper.createQueryMessage(statement, requestSequence);
        try {
            outputStreamLock.lock();
            outputStream.write(queryMessage, 0, queryMessage.length);
            outputStream.flush();
            outputStreamLock.unlock();

            waitForResponse(requestSequence);

            byte[] response = responseMap.get(requestSequence);
            logger.info("Response from DB: {}", CQLMapper.convertToHexString(response, response.length));

            //Validate if we have response for the same sequence as in request
            if (requestSequence != ByteBuffer.wrap(response).getShort(2)) {
                logger.error("Sequence mismatch!!");
            }
        } catch (IOException e) {
            logger.error("Error on Stream operation... ", e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("BusyWait")
    private void waitForResponse(int sequence) {
        while (!responseMap.containsKey(sequence)) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                logger.error("Error while waiting for response", e);
                throw new RuntimeException(e);
            }
        }
    }
}
