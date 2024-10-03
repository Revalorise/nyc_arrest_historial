import org.helper_utility.ZipSerialize;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ZipSerializeTest {
    String testFilePath =
            "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Arrest_Data_Year_to_Date.csv";
    String outputFileName = "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Serialized.zip";

    @Test
    public void TestZipSerialize() throws IOException {
        ZipSerialize zipSerialize = new ZipSerialize();
        zipSerialize.serializeToZIP(testFilePath, outputFileName);
    }
}
