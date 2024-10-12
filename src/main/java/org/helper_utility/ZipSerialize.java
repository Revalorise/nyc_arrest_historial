package org.helper_utility;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipException;

import net.lingala.zip4j.ZipFile;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ZipSerialize {
    private static final Logger logger = LogManager.getLogger(ZipSerialize.class);

    public static void serializeToZIP(String sourceFilePath, String outputFilePath)
        throws IOException {

        File outputFile = new File(outputFilePath);
        if (outputFile.exists()) {
            logger.info("Zip file already exists.. skipping...");
        } else {
            new ZipFile(outputFilePath).addFile(sourceFilePath);
            logger.info("Zip file generated successfully");
        }
    }

    public static void deleteZipFile(String zipFilePath) throws IOException {
        File zipFile = new File(zipFilePath);
        if (zipFile.exists()) {
            zipFile.delete();
            logger.info("Zip file deleted successfully");
        } else {
            logger.info("Zip file does not exist");
        }
    }

    public static void unzipFile(String zipFilePath, String outputFilePath) throws IOException {

        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            zipFile.extractAll(outputFilePath);
            logger.info("Zip extracted successfully");
        } catch (ZipException e) {
            System.out.println(e.getMessage());
        }
    }
}
