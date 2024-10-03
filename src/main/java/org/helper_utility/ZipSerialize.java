package org.helper_utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ZipSerialize {
    private static final Logger logger = LogManager.getLogger(ZipSerialize.class);

    public void serializeToZIP(Path sourceFilePath, String outputFileName)
        throws IOException {

        if (!Files.isRegularFile(sourceFilePath)) {
            System.err.println("Source file does not exist");
        }

        try (var zipOutputStream = new ZipOutputStream(new FileOutputStream(outputFileName));
             var fileInputStream = new FileInputStream(sourceFilePath.toFile());
        ) {
            var zipEntry = new ZipEntry(sourceFilePath.getFileName().toString());
            zipOutputStream.putNextEntry(zipEntry);

            byte[] buffer = new byte[1024];
            int length;
            while ((length = fileInputStream.read(buffer)) > 0) {
                zipOutputStream.write(buffer, 0, length);
            }
            zipOutputStream.closeEntry();
            logger.info("Successfully zipped " + sourceFilePath.getFileName() + " to " + outputFileName);
        }

    }
}
