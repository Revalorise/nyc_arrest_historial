package org.helper_utility;

import java.io.IOException;

import net.lingala.zip4j.ZipFile;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ZipSerialize {
    private static final Logger logger = LogManager.getLogger(ZipSerialize.class);

    public void serializeToZIP(String sourceFilePath, String outputFileName)
        throws IOException {

        new ZipFile(outputFileName).addFile(sourceFilePath);
        logger.info("Zip file generated successfully");

    }
}
