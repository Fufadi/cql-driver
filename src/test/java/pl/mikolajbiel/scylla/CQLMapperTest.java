package pl.mikolajbiel.scylla;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CQLMapperTest {

    @Test
    public void shouldCreateCorrectStartupMessage(){
        byte[] startupMessage = CQLMapper.createStartupMessage();
        byte[] expected = new byte[] {4, 0, 0, 0, 1, 0, 0, 0, 26, 0, 1, 0, 0, 0, 11, 67, 81, 76, 95, 86, 69, 82, 83, 73, 79, 78, 0, 0, 0, 5, 51, 46, 48, 46, 48};
        assertThat(startupMessage).isEqualTo(expected);
    }

    @Test
    public void shouldCreateCorrectExecuteQuery(){
        byte[] queryMessage = CQLMapper.createQueryMessage("INSERT INTO ks.t(a,b,c) VALUES (1,2,3)", 12);
        byte[] expected = new byte[] {4, 0, 0, 12, 7, 0, 0, 0, 47, 0, 0, 0, 38, 73, 78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 107, 115, 46, 116, 40, 97, 44, 98, 44, 99, 41, 32, 86, 65, 76, 85, 69, 83, 32, 40, 49, 44, 50, 44, 51, 41, 0, 0, 0, 0, 0};

        assertThat(queryMessage).isEqualTo(expected);
    }

    @Test
    public void shouldConvertBytesToHex(){
        byte[] input = new byte[]{88, 55, 44, 10, 3, 120};
        String hexResult = CQLMapper.convertToHexString(input, input.length);
        assertThat(hexResult).isEqualTo("58 37 2C 0A 03 78");
    }

}