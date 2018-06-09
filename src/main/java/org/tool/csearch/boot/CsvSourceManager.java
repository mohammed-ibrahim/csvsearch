package org.tool.csearch.boot;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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

                log.error(e.getMessage(), e);

            }

        } else {

//            String foundMd5 = getMd5OfFileContents(waterMarkFile);
//            log.info("Found md5: {}", foundMd5);
            this.indexingRequired = false;
        }
    }

    private String getMd5OfFileContents(Path filePath) throws Exception {

        String md5;

        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {

            md5 = DigestUtils.md5Hex(fis);

        }

        return md5;
    }

    private boolean isLocalMachine() {

        try {

            InetAddress addr = InetAddress.getLocalHost();
            String hostname = addr.getHostName();
            if (hostname.toLowerCase().indexOf("macbook") > -1) {

                return true;
            }

        } catch (Exception e) {

        }

        return false;
    }
}
