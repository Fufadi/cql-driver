package org.example;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MainTest {

    @Test
    public void test() {
        assertThat("TRUE").isNotEmpty();
    }
}