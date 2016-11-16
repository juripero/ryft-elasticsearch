package com.dataart.ryft.disruptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    private static final int READ_CHARS = 43554432;
    private final static int DOCUMENT_COUNT = 18000;
    private final static int FILES_COUNT = 100;

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Pass the input file as the argument");
            System.exit(1);
        }
        String fileName = args[0];
        FileInputStream fileInputStream = null;
        BufferedReader bufferedReader = null;

        File file = null;
        FileWriter fileWriter = null;
        try {

            fileInputStream = new FileInputStream(new File(fileName));
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

            char[] charsAr = new char[READ_CHARS];
            int res = bufferedReader.read(charsAr, 0, READ_CHARS);
            // String line = new String(charsAr);
            // int position = 0;

            for (int i = 1; i < FILES_COUNT; ++i) {
                file = new File("/ryftone/simpleTest/reddit.redditjsonfld");
                // position = 1;
//                 file = new
//                 File("/Users/imasternoy/RyftElastic/reddit.redditjsonfld");
                fileWriter = new FileWriter(file, true);
                try {
                    // while (position % 19 != 0 || res != -1) {
                    // fileWriter.write("{\"index\":{\"_index\":\"reddit\",\"_type\":\"redditType\",\"_id\":"
                    // + position
                    // + "}}");
                    // fileWriter.flush();
                    // fileWriter.write("\n");
                    // fileWriter.flush();
                    fileWriter.write(charsAr);
                    // fileWriter.flush();
                    // fileWriter.write("\n");
                    // fileWriter.flush();
                    // line = new String(charsAr);
                    // position += 1;
                    charsAr = new char[READ_CHARS];
                    res = bufferedReader.read(charsAr);
                    if (res == -1) {
                        return;
                    }
                    // }
                } finally {
                    fileWriter.close();
                }
                if (i % 10 == 0) {
                    System.out.println("Generated" + i + " files");
                }
            }
        } finally {
            fileWriter.close();
            fileInputStream.close();
            bufferedReader.close();
        }
    }
}
