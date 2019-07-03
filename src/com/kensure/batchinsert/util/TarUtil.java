package com.kensure.batchinsert.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;

public class TarUtil {

	public static void targz(String filePath, String id) throws Exception {
		// TODO Auto-generated method stub
		if (new File(filePath).exists()) {
			String archive = archive(filePath, id);// 生成tar包
			compressArchive(archive);// 生成gz包
			FileUtils.deleteQuietly(new File(archive));
		}

	}

	/**
	 * 归档
	 * 
	 * @param entry
	 * @return
	 */
	private static String archive(String entry, String id) throws IOException {
		File file = new File(entry);

		TarArchiveOutputStream tos = new TarArchiveOutputStream(
				new FileOutputStream(file.getAbsolutePath() + id + ".tar"));
		String base = file.getName();
		if (file.isDirectory()) {
			archiveDir(file, tos, base);
		} else {
			archiveHandle(tos, file, base);
		}
		tos.close();
		return file.getAbsolutePath() + id + ".tar";
	}

	/**
	 * 把tar包压缩成gz
	 * 
	 * @param path
	 * @throws IOException
	 * @author yutao
	 * @return
	 * @date 2017年5月27日下午2:08:37
	 */
	public static String compressArchive(String path) throws IOException {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path));

		GzipCompressorOutputStream gcos = new GzipCompressorOutputStream(
				new BufferedOutputStream(new FileOutputStream(path + ".gz")));

		byte[] buffer = new byte[1024];
		int read = -1;
		while ((read = bis.read(buffer)) != -1) {
			gcos.write(buffer, 0, read);
		}
		gcos.close();
		bis.close();
		return path + ".gz";
	}

	/**
	 * 递归处理，准备好路径
	 * 
	 * @param file
	 * @param tos
	 * @param basePath
	 * @throws IOException
	 */
	private static void archiveDir(File file, TarArchiveOutputStream tos, String basePath) throws IOException {
		File[] listFiles = file.listFiles();
		for (File fi : listFiles) {
			if (fi.isDirectory()) {
				archiveDir(fi, tos, basePath + File.separator + fi.getName());
			} else {
				archiveHandle(tos, fi, basePath);
			}
		}
	}

	/**
	 * 具体归档处理（文件）
	 * 
	 * @param tos
	 * @param fi
	 * @param basePath
	 * @throws IOException
	 */
	private static void archiveHandle(TarArchiveOutputStream tos, File fi, String basePath) throws IOException {
		TarArchiveEntry tEntry = new TarArchiveEntry(basePath + File.separator + fi.getName());
		tEntry.setSize(fi.length());
		tos.putArchiveEntry(tEntry);
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fi));
		byte[] buffer = new byte[1024];
		int read = -1;
		while ((read = bis.read(buffer)) != -1) {
			tos.write(buffer, 0, read);
		}
		bis.close();
		tos.closeArchiveEntry();// 这里必须写，否则会失败
	}

}
