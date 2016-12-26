package org.myapp.producer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by hluu on 9/8/16.
 */
public class TweetFileCopier {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: TweetFileCopier <base dir> <sub dir list or ALL> <dest dir");
            System.exit(-1);
        }

        String baseDirStr = args[0];
        String[] subDirStrList = args[1].split(",");
        String destDirStr = args[2];

        Path destDir = Paths.get(destDirStr);
        if (!Files.exists(destDir)) {
            throw new RuntimeException(destDir.toAbsolutePath() + " doesn't exist");
        }

        Path baseDir = Paths.get(baseDirStr);
        if (!Files.exists(baseDir)) {
            throw new RuntimeException(baseDir.toAbsolutePath() + " doesn't exist");
        }

        if (subDirStrList.length == 0) {
            throw new RuntimeException("sub directory list is empty");
        }

        System.out.println("============================================");
        System.out.printf("Base dir: %s%n", baseDir.toAbsolutePath().toString());
        System.out.printf("Sub dir: %s%n", Arrays.asList(subDirStrList));
        System.out.printf("Dest dir: %s%n", destDir.toAbsolutePath().toString());
        System.out.println("============================================");

        int result = copyFiles(baseDir.toFile(), subDirStrList, destDir.toFile());
        System.out.format("=== done copying %d%n", result);

    }

    private static int copyFiles(File baseDir, String[] subDirStrList, File destDir)
        throws IOException {

        List<File> subDirList = new LinkedList<File>();
        if (subDirStrList.length == 1 && "ALL".equals(subDirStrList[0])) {
            // handle the case where subDirStrList is "ALL"
            for (File subDir : baseDir.listFiles()) {
                if (subDir.isDirectory()) {
                    subDirList.add(subDir);
                } else {
                    System.out.printf("*** %s is not a directory. Skipping%n", subDir.getAbsolutePath());
                }
            }
        } else {
            // now build subDirList
            for (String subDirStr : subDirStrList) {
                File subDir = new File(baseDir + File.separator + subDirStr);
                if (!subDir.exists()) {
                    System.out.printf("*** %s doesn't exist. Skipping%n", subDir.getAbsolutePath());
                }
                subDirList.add(subDir);
            }
        }

        System.out.printf("Will processing the following sub directories: %s%n",
                subDirList);

        int numFileCopied = 0;
        for (File dir : subDirList) {

            String subDirName = dir.getName();
            System.out.println("Processing directory: " + subDirName);
            Path outputPath = Paths.get(destDir.getAbsolutePath() + File.separator + subDirName);
            if (Files.exists(outputPath)) {
                System.out.printf("Output dir %s already existed. Skipping%n", outputPath);
                continue;
            } else {
                Files.createDirectory(outputPath);
            }

            for (File f : dir.listFiles()) {
                if (f.isDirectory()) {
                    String timeStamp = f.getName().split("_")[1];
                    System.out.println("\t sub dir: " + f.getAbsolutePath());
                    File[] partFileArr = f.listFiles(new PartFileNameFilter());
                    if (partFileArr.length > 1) {
                        throw new RuntimeException("Too many part files in: " + f.getAbsolutePath());
                    }

                    String destName = String.format("%s%s%s-%s.json", outputPath.toString(),
                            File.separator, subDirName, timeStamp);
                    Path destPath = Paths.get(destName);
                    System.out.printf("\tCopy from %s to %s%n", partFileArr[0], destPath);
                    Files.copy(partFileArr[0].toPath(), destPath);

                    numFileCopied++;
                }
            }
        }

        return numFileCopied;

    }

    private static class PartFileNameFilter implements FilenameFilter {
        public boolean accept(File dir, String name) {
            if (name.startsWith("part-r-00000")) {
                return true;
            } else {
                return false;
            }
        }
    }
}
