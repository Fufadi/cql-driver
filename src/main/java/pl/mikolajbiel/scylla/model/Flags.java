package pl.mikolajbiel.scylla.model;

public class Flags {
    public static final byte NONE = 0x00;
    public static final byte COMPRESSION = 0x01;
    public static final byte TRACING = 0x02;
    public static final byte CUSTOM_PAYLOAD = 0x04;
    public static final byte WARNING = 0x08;
}
