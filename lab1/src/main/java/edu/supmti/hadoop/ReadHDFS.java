package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        if(args.length < 1) {
            System.out.println("Usage: ReadHDFS <file_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(args[0]);
        
        if(!fs.exists(filePath)) {
            System.out.println("File does not exist: " + args[0]);
            System.exit(1);
        }

        FSDataInputStream inStream = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
        String line;
        while((line = br.readLine()) != null) {
            System.out.println(line);
        }

        inStream.close();
        fs.close();
    }
}
