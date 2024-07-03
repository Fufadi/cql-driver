package pl.mikolajbiel.scylla;

import org.apache.log4j.BasicConfigurator;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

    public static void main(String[] args) {
        BasicConfigurator.configure();

        String host = "127.0.0.1"; // "172.17.0.2"
        Session session = Session.connect(new InetSocketAddress(host, 9042));

        Set<Thread> threads = IntStream.range(0, 512)
                .mapToObj(i -> Thread.startVirtualThread(() -> session.execute(String.format("INSERT INTO ks.t(a,b,c) VALUES (%d,%d,%d)", i, i, i))))
                .collect(Collectors.toSet());

        threads.forEach(Main::threadJoin);
    }

    private static void threadJoin(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}