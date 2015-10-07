package com.techklout.util;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.*;

public class TKUtil {
	static Logger logger = Logger.getLogger(TKUtil.class);

	public static String[] listHdfsFiles(String path) {
		String[] files = new String[0];
		FileSystem fs = null;
		try {
			Configuration hadoopConfig = new Configuration();
			hadoopConfig.set("fs.hdfs.impl",
					org.apache.hadoop.hdfs.DistributedFileSystem.class
							.getName());
			hadoopConfig.set("fs.file.impl",
					org.apache.hadoop.fs.LocalFileSystem.class.getName());
			fs = FileSystem.get(new URI(path), hadoopConfig);
			FileStatus[] status = fs.listStatus(new Path(path)); // you need to
																	// pass in
																	// your hdfs
																	// path

			Arrays.sort(status, new Comparator<FileStatus>() {
				public int compare(FileStatus f1, FileStatus f2) {
					return Long.valueOf(f1.getModificationTime()).compareTo(
							f2.getModificationTime());
				}
			});

			files = new String[status.length];
			for (int i = 0; i < status.length; i++) {
				logger.debug(status[i].getPath().getName() + "  Time:"
						+ new Timestamp(status[i].getModificationTime()));
				files[i] = status[i].getPath().getName();
			}
		} catch (Exception e) {
			logger.error("Exception while listing files for path !!! - " + path);
			logger.error(e.getMessage(), e);
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (Exception e) { }
			}
		}
		return files;
	}
	
	
	
	public static boolean moveHdfsFile(String path, String destDir) {
		String[] files = new String[0];
		FileSystem fs = null;
		try {
			Configuration hadoopConfig = new Configuration();
			hadoopConfig.set("fs.hdfs.impl",
					org.apache.hadoop.hdfs.DistributedFileSystem.class
							.getName());
			hadoopConfig.set("fs.file.impl",
					org.apache.hadoop.fs.LocalFileSystem.class.getName());
			fs = FileSystem.get(new URI(path), hadoopConfig);
			Path srcPath = new Path(path);
			Path destPath = new Path(destDir);
			return fs.rename(srcPath, destPath);
			
			
		} catch (Exception e) {
			logger.error("Exception while moving file:" + path + " to dir:" + destDir);
			logger.error(e.getMessage(), e);
			return false;
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (Exception e) { }
			}
		}
		
	}
	
	
	
	public static String[] listFiles(String path) {
		String[] files = new String[0];
		try {
			File dir = new File(path);
			File[] fs = dir.listFiles();

			Arrays.sort(fs, new Comparator<File>(){
			    public int compare(File f1, File f2)
			    {
			        return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
			    } });

			files = new String[fs.length];
			for (int i = 0; i < fs.length; i++) {
				logger.debug(fs[i].getAbsolutePath() + "  Time:"
						+ new Timestamp(fs[i].lastModified()));
				files[i] = fs[i].getName();
			}
		} catch (Exception e) {
			logger.error("Exception while listing files for path !!! - " + path);
			logger.error(e.getMessage(), e);
		}
		return files;
	}
	
	

	public static void main(String[] args) {
		args = new String[1];
		args[0] = "hdfs://ip-172-31-6-162:9000/user/ubuntu/tk/so/posts/";
		if (args.length < 1) {
			System.err.println("Usage: java TKUtil <hdfs dir path>");
			return;
		}
		//TKUtil.listHdfsFiles(args[0]);
		TKUtil.moveHdfsFile("hdfs://ip-172-31-6-162:9000/user/ubuntu/stackexchange/meta-programmers/myPosts.xml", "hdfs://ip-172-31-6-162:9000/user/ubuntu/stackexchange/meta-programmers/posts_done");
	}
}