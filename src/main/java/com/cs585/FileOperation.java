package com.cs585;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileOperation {
    // initialize this class first:     FileOperation fo = new FileOperation();
    // use the function:  eg:           fo.deleteData("hdfs://localhost:9000", "/user/output");


 
    // delete a path 
    // eg:  String HDFS_PATH = "hdfs://localhost:9000";
    //      String FILE_PATH = "/user/output";
	public void deleteData(String HDFS_PATH, Path FILE_PATH) throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
		fileSystem.delete((FILE_PATH), true);
	}

	public void Rename(String HDFS_PATH, Path srcPath, Path desPath) throws Exception{
		FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
		fileSystem.rename(srcPath,desPath);
		fileSystem.close();
	}


    // download data
    // eg:  String HDFS_PATH = "hdfs://localhost:9000";
    //      String FILE_PATH = "/user/output";
	public void downloadData(String HDFS_PATH, String FILE_PATH) throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
		FSDataInputStream in = fileSystem.open(new Path(FILE_PATH));
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
 
    // make a direction 
    // eg:  String HDFS_PATH = "hdfs://localhost:9000";
    //      String DIR_PATH = "/user";
	public   void makeDir(String HDFS_PATH, String DIR_PATH) throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
		fileSystem.mkdirs(new Path(DIR_PATH));
	}
	
    // upload data
    // eg:  String HDFS_PATH = "hdfs://localhost:9000";
    //      String FILE_PATH = "/user/output";
    //      String LOCAL_PATH = "d:/log.txt";
	public void uploadData(String HDFS_PATH, String FILE_PATH, String LOCAL_PATH) throws IOException,
			URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
		FSDataOutputStream out = fileSystem.create(new Path(FILE_PATH));
		InputStream in = new FileInputStream(new File(LOCAL_PATH));
		IOUtils.copyBytes(in, out, 1024, true);
	}
}