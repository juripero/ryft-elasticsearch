import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    private final static int DOCUMENT_COUNT = 18000;
    private final static int FILES_COUNT = 1200;

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Pass the input file as the argument");
            System.exit(1);
        }
        if(args.length == 1){
            System.err.println("Pass the output directory/filePrefix as the argument");
            System.exit(1); 
        }
        if(args.length == 2){
            System.err.println("Pass fileCount as the argument");
            System.exit(1); 
        }
        
        String fileName = args[0];
        String outputFileName = args[1];
        int fileCount = Integer.valueOf(args[2]);
        
        FileInputStream fileInputStream = null;
        BufferedReader bufferedReader = null;

        try {
            fileInputStream = new FileInputStream(new File(fileName));
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

            String line = bufferedReader.readLine();
            int position = 0;

            for (int i = 1; i < fileCount; ++i) {
                File file = new File(outputFileName+ i);
                FileWriter fileWriter = null;
                try {
                    fileWriter = new FileWriter(file, true);
                    while (line != null && position != DOCUMENT_COUNT * i) {
                        fileWriter.write("{\"index\":{\"_index\":\"reddit\",\"_type\":\"redditType\",\"_id\":"
                                + position + "}}");
                        fileWriter.flush();
                        fileWriter.write("\n");
                        fileWriter.flush();
                        fileWriter.write(line);
                        fileWriter.flush();
                        fileWriter.write("\n");
                        fileWriter.flush();

                        line = bufferedReader.readLine();
                        if(line == null){
                            return;
                        }
                        ++position;
                    }
                    if(i % 100 == 0){
                        System.out.println("Generated"+ i +" files");
                    }
                } finally {
                    fileWriter.close();
                }
            }
        } finally {
            fileInputStream.close();
            bufferedReader.close();
        }
    }
}
