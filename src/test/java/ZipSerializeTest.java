import org.helper_utility.ZipSerialize;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ZipSerializeTest {
    Path testFilePath = Path.of(
            "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Arrest_Data_Year_to_Date.csv");
    String outputFileName = "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Serialized.zip";

    @Test
    public void testZipSerialize() throws IOException {
        ZipSerialize zipSerialize = new ZipSerialize();
        zipSerialize.serializeToZIP(testFilePath, outputFileName);
    }
}
