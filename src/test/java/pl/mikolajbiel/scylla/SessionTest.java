package pl.mikolajbiel.scylla;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class SessionTest {
    private static final String HOST = "testLocalHost";
    private final SocketFactory socketFactoryMock = mock(SocketFactory.class);
    private final Socket socketMock = mock(Socket.class);
    private final InputStream inputStreamMock = mock(InputStream.class);
    private final OutputStream outputStreamMock = mock(OutputStream.class);

    @BeforeEach
    public void setup() throws IOException {
        when(socketFactoryMock.createSocket(eq(HOST), anyInt())).thenReturn(socketMock);
        when(socketMock.getInputStream()).thenReturn(inputStreamMock);
        when(socketMock.getOutputStream()).thenReturn(outputStreamMock);
    }

    @Test
    public void shouldCreateNewSessionObjectAndConnectToSocket() throws IOException {
        Session session = Session.connect(socketFactoryMock, new InetSocketAddress(HOST, 9042));

        assertThat(session).isNotNull();
        verify(socketFactoryMock).createSocket(HOST, 9042);
        verify(socketMock).getInputStream();
        verify(socketMock).getOutputStream();
        verify(outputStreamMock).write(any(), eq(0), eq(35));
        verify(inputStreamMock, atLeastOnce()).read(any());
    }

    @Test
    public void shouldDisconnectWhenSecondConnectCalled() throws IOException {
        Session session = Session.connect(socketFactoryMock, new InetSocketAddress(HOST, 9042));
        Session newSession = Session.connect(socketFactoryMock, new InetSocketAddress(HOST, 9044));

        assertThat(newSession).isNotEqualTo(session);
        verify(socketFactoryMock, times(2)).createSocket(eq(HOST), anyInt());
        verify(socketMock).close();
        verify(inputStreamMock).close();
        verify(outputStreamMock).close();
    }

    @Test
    void shouldExecuteProvidedQueryAndSendItToSocket() throws IOException {
        Session session = Session.connect(socketFactoryMock, new InetSocketAddress(HOST, 9042));

        session.execute("INSERT INTO ks.t(a,b,c) VALUES (1,2,3)");

        byte[] expectedByteArray = {0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1A, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0B, 0x43, 0x51, 0x4C, 0x5F, 0x56, 0x45, 0x52, 0x53, 0x49, 0x4F, 0x4E, 0x00, 0x00, 0x00, 0x05, 0x33, 0x2E, 0x30, 0x2E, 0x30};
        verify(outputStreamMock).write(expectedByteArray, 0, 35);
        verify(inputStreamMock, atLeast(2)).read(any());
    }
}