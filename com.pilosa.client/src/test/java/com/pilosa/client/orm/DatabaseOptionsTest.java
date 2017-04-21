package com.pilosa.client.orm;

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.UnitTest;
import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class DatabaseOptionsTest {
    @Test
    public void testBuilder() {
        DatabaseOptions options = DatabaseOptions.builder()
                .build();
        compare(options, "col_id", TimeQuantum.NONE);

        options = DatabaseOptions.builder()
                .setColumnLabel("random_lbl")
                .build();
        compare(options, "random_lbl", TimeQuantum.NONE);

        options = DatabaseOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, "col_id", TimeQuantum.YEAR_MONTH_DAY_HOUR);

        options = DatabaseOptions.builder()
                .setColumnLabel("some_label")
                .setTimeQuantum(TimeQuantum.DAY)
                .build();
        compare(options, "some_label", TimeQuantum.DAY);
    }

    @Test(expected = PilosaException.class)
    public void testInvalidColumnLabel() {
        DatabaseOptions.builder()
                .setColumnLabel("#Justa an invalid label!")
                .build();
    }

    private void compare(DatabaseOptions options, String targetColumnLabel, TimeQuantum targetTimeQuantum) {
        assertEquals(targetColumnLabel, options.getColumnLabel());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
    }
}
