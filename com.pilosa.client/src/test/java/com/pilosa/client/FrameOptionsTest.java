package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.FrameOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class FrameOptionsTest {
    @Test
    public void testBuilder() {
        FrameOptions options = FrameOptions.builder()
                .build();
        compare(options, "id", TimeQuantum.NONE);

        options = FrameOptions.builder()
                .setRowLabel("the_row_label")
                .build();
        compare(options, "the_row_label", TimeQuantum.NONE);

        options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, "id", TimeQuantum.YEAR_MONTH_DAY_HOUR);

        options = FrameOptions.builder()
                .setRowLabel("someid")
                .setTimeQuantum(TimeQuantum.YEAR)
                .build();
        compare(options, "someid", TimeQuantum.YEAR);
    }

    @Test(expected = PilosaException.class)
    public void testInvalidRowLabel() {
        FrameOptions.builder()
                .setRowLabel("#Justa an invalid label!")
                .build();
    }

    private void compare(FrameOptions options, String targetRowLabel, TimeQuantum targetTimeQuantum) {
        assertEquals(targetRowLabel, options.getRowLabel());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
    }
}
