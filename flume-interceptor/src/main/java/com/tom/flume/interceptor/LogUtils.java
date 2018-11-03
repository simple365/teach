package com.tom.flume.interceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {
    private  static Logger logger= LoggerFactory.getLogger(LogUtils.class);
    /**
     * 日志检查，正常的log会返回true，错误的log返回false
     * @param log
     */
    public static boolean validateReportLog(String log){
        try {
//        日志的格式是 ip | 时间戳| json串
            if (log.split("\\|").length < 3) {
                return false;
            }
            String[] logArray=log.split("\\|");
//        检查第一串是否为ip,有时候ip没拿到也没有关系。许多验证类框架里面会自带验证类，如 StringUtils
            if(StringUtils.isNotEmpty(logArray[0])){
                String[] ipArray=logArray[0].split("\\.");
                if(ipArray.length!=4){
                    return false;
                }else{
                    for(String num:ipArray){
                        if(!NumberUtils.isDigits(num)){
                            return false;
                        }
                    }
                }
            }
//            检查第二串是否为时间戳
            if(logArray[1].length()!=13 || !NumberUtils.isDigits(logArray[1])){
                return false;
            }
//            第三串是否为正确的json,这里我们就粗略的检查了，有时候我们需要从后面来发现json传错的数据，做分析
            if(!logArray[2].trim().startsWith("{")||!logArray[2].trim().endsWith("}")){
                return false;
            }
        }catch (Exception e){
//            错误日志打印，需要查看
            logger.error("parse error,message is:"+log);
            logger.error(e.getMessage());
            return false;
        }
        return  true;
    }


}
