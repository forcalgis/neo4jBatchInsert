package com.kensure.batchinsert.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class ArgsConfig {

	private static final Log logger = LogFactory.getLog(ArgsConfig.class);

	public static final Map<String, String> ARGSMAP = new HashMap<String, String>();

	public static final String MAPPING = "mapping";
	public static final String RUNMODE = "runmode";
	public static final String STARTTIME = "starttime";
	public static final String ENDTIME = "endtime";
	public static final String BREAKPOINT = "BreakPoint.time";
	public static final String HISTORY = "history";
	public static final String INCREMENT = "increment";
	public static final String CALLBACK = "callback";
	public static final String CALLBACKURL = "callback_url";

	public static final void initArgs(String[] args) throws Exception {

		if (args == null || args.length <= 0 || args[0] == null || args[0].length() <= 0) {
			throw new Exception("请输入配置文件地址！");
		}
		String filepath = args[1];
		logger.info("配置文件地址：" + filepath);

		// 初始化
		ARGSMAP.put(MAPPING, filepath);
		ARGSMAP.put(RUNMODE, HISTORY);
		ARGSMAP.put(STARTTIME, "0");
		ARGSMAP.put(ENDTIME, String.valueOf(System.currentTimeMillis()));
		ARGSMAP.put(CALLBACK, "false");

		// {"is_callback":"true","callback_url":"http://53.112.5.14:8086/kensureToolSet/rest/executor/result/callback?execute_id=tongwb_1537111800001&callback_status=","is_increase":"false","start_range":"","stop_range":""}
		if (args.length >= 4) {
			logger.error("参数明细：" + args[3]);
			JSONObject paramObj = JSONObject.fromObject(args[3]);

			if (paramObj.containsKey("is_increase")) {
				// 是否增量
				String isIncrease = paramObj.getString("is_increase");
				// 设置是否增量标识
				if ("true".equalsIgnoreCase(isIncrease)) {
					ARGSMAP.put(RUNMODE, INCREMENT);
					String breakPoint = null;
					try {
						breakPoint = FileUtils.readFileToString(new File(BREAKPOINT));
					} catch (Exception e) {
						
					}
					if (null != breakPoint) {
						ARGSMAP.put(STARTTIME, String.valueOf(DateUtil.s2data(breakPoint).getTime()));
					}
				}
			}

			// 执行数据增量范围
			if (paramObj.containsKey("start_range")) {
				String start_range = paramObj.getString("start_range");
				if (start_range != null && start_range.length() > 0) {
					ARGSMAP.put(STARTTIME, String.valueOf(DateUtil.s2data(start_range).getTime()));
				}
			}

			if (paramObj.containsKey("stop_range")) {
				String stop_range = paramObj.getString("stop_range");
				if (stop_range != null && stop_range.length() > 0) {
					ARGSMAP.put(ENDTIME, String.valueOf(DateUtil.s2data(stop_range).getTime()));
				}
			}

			if (paramObj.containsKey("is_callback")) {
				// 是否回调
				String isCallback = paramObj.getString("is_callback");
				if ("true".equalsIgnoreCase(isCallback) && paramObj.containsKey("callback_url")) {
					String callback_url = paramObj.getString("callback_url");
					logger.error("是否执行回调:" + isCallback);
					logger.error("回调地址:" + callback_url);
					ARGSMAP.put(CALLBACK, isCallback);
					ARGSMAP.put(CALLBACKURL, callback_url);
				}
			}
		}
	}
}
