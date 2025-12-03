package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        if(args.length < 2) {
            System.out.println("Usage: HDFSWrite <file_path> <content>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(args[0]);

        if(!fs.exists(filePath)) {
            FSDataOutputStream outStream = fs.create(filePath);
            outStream.writeUTF(args[1]);
            outStream.close();
            System.out.println("File created: " + args[0]);
        } else {
            System.out.println("File already exists: " + args[0]);
        }

        fs.close();
    }
}
