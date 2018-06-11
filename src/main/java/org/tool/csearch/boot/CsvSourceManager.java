package org.tool.csearch.boot;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.tool.csearch.common.Constants;
import org.tool.csearch.common.Timer;
import org.tool.csearch.log.Logger;

import lombok.Data;

@Data
public class CsvSourceManager {

    private Path dropPath;

    private Boolean indexingRequired;

    public CsvSourceManager(Path filePath) throws Exception {
        this.indexingRequired = true;

        String md5File = getMd5OfFileContents(filePath);

        String md5Name = DigestUtils.md5Hex(filePath.toAbsolutePath().toString());

        String baseDir = "";

        baseDir = isLocalMachine() ? "/tmp" : System.getProperty("java.io.tmpdir");
        dropPath = Paths.get(baseDir, "csvsearch", md5Name, md5File);

        Path waterMarkFile = Paths.get(dropPath.toString(), Constants.WATER_MARK_FILE);

        if (!waterMarkFile.toFile().isFile()
                || !Constants.WATER_MARK_FILE_EXPECTED_MD5.equals(getMd5OfFileContents(waterMarkFile))) {

            try {

                File dropPathFile = dropPath.toFile();

                if (dropPathFile.exists() && dropPathFile.isDirectory()) {
                    FileUtils.deleteDirectory(dropPathFile);
                }

            } catch (Exception e) {

                Logger.info(e.getMessage() + " " + e);

            }

        } else {

            this.indexingRequired = false;
        }
    }

    private String getMd5OfFileContents(Path filePath) throws Exception {

        String md5;
        Timer timer = new Timer();

        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {

            md5 = DigestUtils.md5Hex(fis);

        }

        Logger.debug(String.format("Time taken to compute hash: %s", timer.end().toString()));
        return md5;
    }

    private boolean isLocalMachine() {

        try {

            String os = System.getProperty("os.name").toLowerCase();

            if (os.indexOf("mac") >= 0) {
                return true;
            }

            // InetAddress addr = InetAddress.getLocalHost();
            // String hostname = addr.getHostName();
            // String hostname = getComputerName();
            // if (hostname.toLowerCase().indexOf("macbook") > -1) {
            //
            // return true;
            // }

        } catch (Exception e) {

        }

        return false;
    }
}
