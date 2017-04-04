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
        FrameOptions options = new FrameOptions.Builder()
                .build();
        compare(options, "id", TimeQuantum.NONE);

        options = new FrameOptions.Builder()
                .setRowLabel("the_row_label")
                .build();
        compare(options, "the_row_label", TimeQuantum.NONE);

        options = new FrameOptions.Builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, "id", TimeQuantum.YEAR_MONTH_DAY_HOUR);

        options = new FrameOptions.Builder()
                .setRowLabel("someid")
                .setTimeQuantum(TimeQuantum.YEAR)
                .build();
        compare(options, "someid", TimeQuantum.YEAR);
    }

    @Test(expected = PilosaException.class)
    public void testInvalidRowLabel() {
        new FrameOptions.Builder()
                .setRowLabel("#Justa an invalid label!")
                .build();
    }

    private void compare(FrameOptions options, String targetRowLabel, TimeQuantum targetTimeQuantum) {
        assertEquals(targetRowLabel, options.getRowLabel());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
    }
}
