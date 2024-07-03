package pl.mikolajbiel.scylla;

import pl.mikolajbiel.scylla.model.Flags;
import pl.mikolajbiel.scylla.model.Opcode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CQLMapper {

    private static final byte protocolVersion = 0x04;
    private static final Logger logger = LoggerFactory.getLogger(CQLMapper.class);

    public static byte[] createStartupMessage(){
        ByteBuffer buffer = ByteBuffer.allocate(512);
        buffer.put(protocolVersion);
        buffer.put(Flags.NONE);
        buffer.putShort((short) 0x0000); // Sequence
        buffer.put(Opcode.STARTUP);

        ByteBuffer bodyBuffer = ByteBuffer.allocate(512);
        bodyBuffer.putShort((short) 1); // Number of options
        putStringToBuffer(bodyBuffer, "CQL_VERSION");
        putStringToBuffer(bodyBuffer, "3.0.0");
        bodyBuffer.flip();

        buffer.putInt(bodyBuffer.remaining());
        buffer.put(bodyBuffer);
        buffer.flip();
        return getFilledBufferAsArray(buffer);
    }

    public static byte[] createQueryMessage(String statement, int sequence){
        ByteBuffer buffer = ByteBuffer.allocate(512);
        buffer.put(protocolVersion);
        buffer.put(Flags.NONE);
        buffer.putShort((short) sequence);
        buffer.put(Opcode.QUERY);

        // Body of QUERY message
        ByteBuffer bodyBuffer = ByteBuffer.allocate(512);
        putStringToBuffer(bodyBuffer, statement);
        bodyBuffer.putInt(0); // Consistency level (ONE)
        bodyBuffer.put(Flags.NONE); // Query flags

        bodyBuffer.flip();
        buffer.putInt(bodyBuffer.remaining());
        buffer.put(bodyBuffer);
        buffer.flip();

        byte[] bufferAsArray = getFilledBufferAsArray(buffer);
        logger.info("Request to DB: {}", convertToHexString(bufferAsArray, bufferAsArray.length));

        return bufferAsArray;
    }

    private static byte[] getFilledBufferAsArray(ByteBuffer byteBuffer){
        byte[] remaining = new byte[byteBuffer.remaining()];
        byteBuffer.get(remaining);

        return remaining;
    }

    private static void putStringToBuffer(ByteBuffer buffer, String str) {
        buffer.putInt(str.length());
        buffer.put(str.getBytes());
    }

    public static String convertToHexString(byte[] bytes, int size){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(String.format("%02X ", bytes[i]));
        }
        return sb.toString().trim();
    }
}
