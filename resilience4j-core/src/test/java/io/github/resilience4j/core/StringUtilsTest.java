package io.github.resilience4j.core;

import org.junit.Test;

public class StringUtilsTest {

    @Test
    public void testString() {
        assertThat(StringUtils.isNotEmpty("bla")).isEqualTo(true);
    }

    @Test
    public void testEmptyString() {
        assertThat(StringUtils.isNotEmpty("")).isEqualTo(false);
    }

    @Test
    public void testNull() {
        assertThat(StringUtils.isNotEmpty(null)).isEqualTo(false);
    }
}
