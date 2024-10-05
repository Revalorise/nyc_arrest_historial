package org.helper_utility;

import java.io.File;
import java.io.IOException;

import net.lingala.zip4j.ZipFile;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ZipSerialize {
    private static final Logger logger = LogManager.getLogger(ZipSerialize.class);

    public void serializeToZIP(String sourceFilePath, String outputFilePath)
        throws IOException {

        File outputFile = new File(outputFilePath);
        if (outputFile.exists()) {
            logger.info("Zip file already exists");
        } else {
            new ZipFile(outputFilePath).addFile(sourceFilePath);
            logger.info("Zip file generated successfully");
        }
    }

    public void deleteZipFile(String zipFilePath) throws IOException {
        File zipFile = new File(zipFilePath);
        if (zipFile.exists()) {
            zipFile.delete();
            logger.info("Zip file deleted successfully");
        } else {
            logger.info("Zip file does not exist");
        }
    }
}
