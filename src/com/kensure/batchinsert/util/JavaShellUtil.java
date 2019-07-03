package com.kensure.batchinsert.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JavaShellUtil {

	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(JavaShellUtil.class);
	private static final Log LOGGER = LogFactory.getLog(JavaShellUtil.class);

	private static final DateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String executeShell(String shellCommand) {
		StringBuffer stringBuffer = new StringBuffer();
		BufferedReader bufferedReader = null;
		BufferedReader errorReader = null;
		// 格式化日期时间，记录日志时使用
		StringBuffer result = new StringBuffer();
		try {
			stringBuffer.append(DATEFORMAT.format(new Date())).append("准备执行Shell命令 [").append(shellCommand)
					.append("]\r\n");
			Process process = null;
			String[] cmd = { "/bin/sh", "-c", shellCommand };
			// 执行Shell命令
			process = Runtime.getRuntime().exec(cmd);
			if (process != null) {
				// stringBuffer.append("进程号：").append(process.toString()).append("\r\n");
				// bufferedReader用于读取Shell的输出内容
				bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()), 1024);
				errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()), 1024);
				process.waitFor();
			} else {
				stringBuffer.append("没有pid\r\n");
				result.append("error").append("\r\n");
				result.append("没有pid\r\n");
			}
			stringBuffer.append(DATEFORMAT.format(new Date())).append("Shell命令执行完毕-->执行结果为:");
			String line = null;
			// 读取Shell的输出内容，并添加到stringBuffer中
			while (bufferedReader != null && (line = bufferedReader.readLine()) != null) {
				stringBuffer.append("\r\n").append(line);
				result.append("ok").append("\r\n");
				result.append(line);
			}

			while (errorReader != null && (line = errorReader.readLine()) != null) {
				stringBuffer.append("\r\n").append(line);
				result.append("error").append("\r\n");
				result.append(line);
			}
		} catch (Exception ioe) {
			stringBuffer.append("执行Shell命令时发生异常：\r\n").append(ioe.getMessage()).append("\r\n");
			result.append("error").append("\r\n");
			result.append(ioe.getMessage()).append("\r\n");
		} finally {
			LOGGER.debug(stringBuffer.toString());
			if (bufferedReader != null) {
				IOUtils.closeQuietly(bufferedReader);
			}
			if (errorReader != null) {
				IOUtils.closeQuietly(errorReader);
			}
		}
		return result.toString();
	}

	public static void main(String[] args) throws IOException {
		System.out.println("....");
		System.out.println(
				JavaShellUtil.executeShell("cat /usr/local/share/neo4j/conf/neo4j.conf | grep dbms.directories.data"));
	}

}
