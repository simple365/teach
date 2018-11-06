package com.tom.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    /**
     * 现在只支持字符串格式的内容
     *
     * @param line
     * @param key
     * @param defaultVal
     * @return
     */
    /**
     * jsonKeys是对应json里面的key，这个是要区分大小写的
     *
     * @param line
     * @param jsonKeysString
     * @return
     */
    public String evaluate(String line, String jsonKeysString) {
        String[] jsonKeys = jsonKeysString.split(",");
        String splitSign = "\t";
        StringBuilder sb = new StringBuilder();
        JSONObject json = null;
        if (StringUtils.isBlank(line)) {
            for (int i = 0; i < jsonKeys.length; i++) {
                sb.append("" + splitSign);
            }
        } else {
            try {
//              提取公共字段
                String[] logContents = line.split("\\|");
                String serverTime = "";
                String ip = "";
                String jsonline = "";
                // 判斷是否出现非法日志，一般都是3个
                if (logContents.length == 3) {
                    serverTime = logContents[1];
                    ip = logContents[0];
                    jsonline = logContents[2];
                } else {
                    return "";
                }
                if (StringUtils.isNotBlank(jsonline)) {
                    json = new JSONObject(jsonline);
                }
                if (json != null) {
                    JSONObject basep = json.getJSONObject("cm");
                    for (int i = 0; i < jsonKeys.length; i++) {
                        String fieldName = jsonKeys[i].trim();
                        sb.append(getValue(basep,fieldName,"")).append(splitSign);
                    }
                    }else {
                        sb.append(splitSign);
                    }
//                    添加业务字段
                    sb.append(json.getString("et")).append(splitSign);
                    sb.append(serverTime).append(splitSign)
                            .append(ip);
            } catch (Exception e) {
                System.err.println(line);
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

    /**
     * 获取json里面的值,自定义默认值，不存在这个key不会报错
     * @param json
     * @param key
     * @param defValue
     * @return
     */
    public static String getValue(JSONObject json, String key, String defValue) {
        if (json == null)
            return defValue;
        try {
            if (json.has(key)) {
                return json.getString(key);
            }else {
                return defValue;
            }
        } catch (JSONException e) {
        }
        return defValue;
    }

    public static void main(String[] args) {
        String line = "10.86.206.155|1541217850324|{\"cm\":{\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"uid\":\"u8739\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"newsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";
        String x = new BaseFieldUDF().evaluate(line, "uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }

}
