package com.kensure.batchinsert.util;

import java.io.File;
import java.util.ArrayList;

public class FileListUtil { 

	private  ArrayList<File> filelist = new ArrayList<File>();
	
	private String path = "lib/";


	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}

	/*
	 * 通过递归得到某一路径下所有的目录及其文件
	 */
	public void getFiles(String filePath) {
		File root = new File(filePath);
		File[] files = root.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				/*
				 * 递归调用
				 */
				getFiles(file.getAbsolutePath());
				System.out.println("显示" + filePath + "下所有子目录及其文件" + file.getAbsolutePath());
			} else {
				filelist.add(file);
				System.out.println("显示" + filePath + "下所有子目录" + file.getAbsolutePath());
			}
		}
	}
	
	public String fileListString() {
		StringBuffer strBuffer = new StringBuffer();
		int index = 0;
		for (File file : filelist) {
			if ( index > 0 ) {
				strBuffer.append(" \n  ");
			}
			index ++;
			strBuffer.append(path).append(file.getName());
		}
		return strBuffer.toString();
	}
	

	public static void main(String[] args) throws Exception {

		try {
			if (args == null || args.length <= 0 || args[0] == null || args[0].length() <= 0) {
				throw new Exception("请输入文件夹目录！");
			}
			String filePath = args[0];
			FileListUtil fileList = new FileListUtil();
			fileList.getFiles(filePath);
			if ( args.length == 2 ) {
				fileList.setPath(args[1]);
			}
			System.out.println(fileList.fileListString());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
