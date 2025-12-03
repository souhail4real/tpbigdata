package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        if(args.length != 3) {
            System.out.println("Usage: HadoopFileStatus <path> <filename> <new_name>");
            System.exit(1);
        }

        String dir = args[0];
        String filename = args[1];
        String newname = args[2];

        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(conf);
            Path filepath = new Path(dir, filename);

            if(!fs.exists(filepath)) {
                System.out.println("File does not exist!");
                System.exit(1);
            }

            FileStatus status = fs.getFileStatus(filepath);

            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("Permission: " + status.getPermission());
            System.out.println("Replication: " + status.getReplication());
            System.out.println("Block Size: " + status.getBlockSize());

            BlockLocation[] blocks =
                fs.getFileBlockLocations(status, 0, status.getLen());

            for(BlockLocation b : blocks) {
                System.out.println("Block offset: " + b.getOffset());
                System.out.println("Block length: " + b.getLength());
                System.out.print("Hosts: ");
                for(String h : b.getHosts()) System.out.print(h + " ");
                System.out.println();
            }

            fs.rename(filepath, new Path(dir, newname));
            System.out.println("File renamed successfully!");

            fs.close();

        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}
