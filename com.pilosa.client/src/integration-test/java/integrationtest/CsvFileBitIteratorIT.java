package integrationtest;

import com.pilosa.client.Bit;
import com.pilosa.client.CsvFileBitIterator;
import com.pilosa.client.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class CsvFileBitIteratorIT {
    @Test
    public void readFromCsvTest() throws FileNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL uri = loader.getResource("sample1.csv");
        if (uri == null) {
            fail("sample1.csv not found");
        }
        CsvFileBitIterator iterator = CsvFileBitIterator.fromPath(uri.getPath());
        List<Bit> bits = new ArrayList<>(3);
        while (iterator.hasNext()) {
            bits.add(iterator.next());
        }
        List<List<Long>> targetValues = Arrays.asList(
                Arrays.asList(1L, 10L, 683793200L),
                Arrays.asList(5L, 20L, 683793300L),
                Arrays.asList(3L, 41L, 683793385L));
        List<Bit> target = new ArrayList<>(3);
        for (List<Long> item : targetValues)
            target.add(Bit.create(item.get(0), item.get(1), item.get(2)));
        assertEquals(3, bits.size());
        assertEquals(target, bits);

        // to get %100 test coverage
        assertFalse(iterator.hasNext());
        iterator.remove();
    }
}
