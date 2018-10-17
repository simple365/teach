package com.tom.business;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 */
public class App {

	private final static Logger logger = LoggerFactory.getLogger(App.class);
	static Random rand = new Random();

	public static void main(String[] args) {
		String logStr = "%s|{\"appkey\":\"browser\", \"he\": {\"userId\": \"%s\", \"area\": \"BR\", \"appVersion\": \"V1.0.2\",\"appTime\": \"%s\"}, \"et\": [{\"eventName\": \"display\", \"kv\": {\"action\": \"1\",\"newsId\": \"%s\"}},"
				+ "{\"eventName\": \"background\",\"kv\": {\"createTime\": \"%s\"}},{\"eventName\": \"foreground\",\"kv\": {\"createTime\": \"%s\"}}]}";
		String backgroundStr = "%s|{\"appkey\":\"browser\", \"he\": {\"userId\": \"%s\", \"area\": \"BR\", \"appVersion\": \"V1.0.2\",\"appTime\": \"%s\"}, \"et\": [{\"eventName\": \"background\",\"kv\": {\"createTime\": \"%s\"}},{\"eventName\": \"display\", \"kv\": {\"action\": \"2\",\"newsId\": \"%s\"}}]}";
		int loop_len = args.length > 1 ? Integer.parseInt(args[1]) : 10 * 10000;
		long millis = System.currentTimeMillis();
		int uid_length = args.length > 2 ? Integer.parseInt(args[2]) : 4;
		int newsid_length = args.length > 3 ? Integer.parseInt(args[3]) : 4;
		long x = 0;
		while (x < loop_len) {
			long forground_createtime = millis - rand.nextInt(99999999);
			long background_createtime = millis - rand.nextInt(99999999);
			long log_time = millis - rand.nextInt(99999999);
			// # 用户id是u，newsid 是 n
			String user_id = 'u' + getRandomDigits(uid_length);
			String news_id = 'n' + getRandomDigits(newsid_length);
			logger.info(String.format(logStr, log_time, user_id, forground_createtime, news_id,
					background_createtime, forground_createtime));
			user_id = 'u' + getRandomDigits(uid_length);
			news_id = 'n' + getRandomDigits(newsid_length);
			logger.info(String.format(backgroundStr, log_time, user_id, forground_createtime,
					background_createtime, news_id));
			x++;
		}
	}

	static String getRandomDigits(int leng) {
		String result = "";
		for (int i = 0; i < leng; i++) {
			result += rand.nextInt(10);
		}
		return result;
	}
}
