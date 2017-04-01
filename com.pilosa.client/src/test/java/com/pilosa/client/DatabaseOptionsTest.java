package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class DatabaseOptionsTest {
    @Test
    public void testBuilder() {
        DatabaseOptions options = new DatabaseOptions.Builder()
                .build();
        compare(options, "col_id", TimeQuantum.NONE);

        options = new DatabaseOptions.Builder()
                .setColumnLabel("random_lbl")
                .build();
        compare(options, "random_lbl", TimeQuantum.NONE);

        options = new DatabaseOptions.Builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, "col_id", TimeQuantum.YEAR_MONTH_DAY_HOUR);

        options = new DatabaseOptions.Builder()
                .setColumnLabel("some_label")
                .setTimeQuantum(TimeQuantum.DAY)
                .build();
        compare(options, "some_label", TimeQuantum.DAY);
    }

    @Test(expected = PilosaException.class)
    public void testInvalidColumnLabel() {
        new DatabaseOptions.Builder()
                .setColumnLabel("#Justa an invalid label!")
                .build();
    }

    private void compare(DatabaseOptions options, String targetColumnLabel, TimeQuantum targetTimeQuantum) {
        assertEquals(targetColumnLabel, options.getColumnLabel());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
    }
}
