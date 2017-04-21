package com.pilosa.client.orm;

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.UnitTest;
import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class FrameOptionsTest {
    @Test
    public void testBuilder() {
        FrameOptions options = FrameOptions.builder()
                .build();
        compare(options, "id", TimeQuantum.NONE, false);

        options = FrameOptions.builder()
                .setRowLabel("the_row_label")
                .build();
        compare(options, "the_row_label", TimeQuantum.NONE, false);

        options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, "id", TimeQuantum.YEAR_MONTH_DAY_HOUR, false);

        options = FrameOptions.builder()
                .setRowLabel("someid")
                .setTimeQuantum(TimeQuantum.YEAR)
                .setInverseEnabled(true)
                .build();
        compare(options, "someid", TimeQuantum.YEAR, true);
    }

    @Test(expected = PilosaException.class)
    public void testInvalidRowLabel() {
        FrameOptions.builder()
                .setRowLabel("#Just an invalid label!")
                .build();
    }

    private void compare(FrameOptions options, String targetRowLabel,
                         TimeQuantum targetTimeQuantum, boolean targetInverseEnabled) {
        assertEquals(targetRowLabel, options.getRowLabel());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
        assertEquals(targetInverseEnabled, options.isInverseEnabled());
    }
}
