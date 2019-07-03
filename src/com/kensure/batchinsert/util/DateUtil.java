package com.kensure.batchinsert.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class DateUtil {

	public static final String SDFSTRING = "yyyy-MM-dd HH:mm:ss";

	private static final SimpleDateFormat sdf = new SimpleDateFormat(SDFSTRING);

	public static final Date s2data(String dateSource) throws ParseException {
		return sdf.parse(dateSource);
	}

	public static final String d2string(Date data) {
		return sdf.format(data);
	}

	
	public static final String d2string(String format,Date data) {
		SimpleDateFormat mysdf = new SimpleDateFormat(format);
		return mysdf.format(data);
	}
	
	public static void main(String[] args) throws ParseException {
		System.out.println(DateUtil.s2data(DateUtil.d2string(new Date(1522385166705L))).getTime());
		System.out.println(DateUtil.s2data("2018-09-12 10:23:12").getTime());
		
		System.out.println(DateUtil.d2string("yyyyMMddHHmmss",new Date()));
		System.out.println(DateUtil.d2string("yyyyMMddHHmmss",new Date(Long.parseLong("1522385166705"))));
		System.out.println(DateUtil.d2string(new Date()));
		System.out.println("是".contains("不是"));
	}
}
