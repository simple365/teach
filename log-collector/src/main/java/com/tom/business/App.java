package com.tom.business;

import java.util.Random;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 生产测试日志的
 */
public class App {

    private final static Logger logger = LoggerFactory.getLogger(App.class);
    static Random rand = new Random();

    /**
     * 1.appkey
     * 2.循环遍历次数
     * 3.uid的长度，默认是4
     * 4.新闻id的长度，默认是4
     * @param args
     */
    public static void main(String[] args) {
        System.out.println(args[0]);
//       appkey的名称
        String appkey = args.length > 0 ?args[0] : "weather";
//		循环遍历次数
        int loop_len = args.length > 1 ? Integer.parseInt(args[1]) : 10 * 10000;
//		uid的长度
        int uid_length = args.length > 2 ? Integer.parseInt(args[2]) : 4;
//		新闻id的长度
        int newsid_length = args.length > 3 ? Integer.parseInt(args[3]) : 4;
        for(int i=0;i<loop_len;i++) {
            JSONObject json = new JSONObject();
            json.put("ap", appkey);
            json.put("cm", generateComFields(uid_length));
            JSONArray eventsArray = new JSONArray();
//        新闻点击，展示
            if (rand.nextBoolean()) {
                eventsArray.add(generateDisplay(newsid_length));
            }
//        新闻详情页
            if (rand.nextBoolean()) {
                eventsArray.add(generateNewsDetail(newsid_length));
            }
//        新闻列表页
            if (rand.nextBoolean()) {
                eventsArray.add(generateNewList());
            }
//        广告
            if (rand.nextBoolean()) {
                eventsArray.add(generateAd());
            }
//        应用启动
            if (rand.nextBoolean()) {
                eventsArray.add(generateStart());
            }
//        消息通知
            if (rand.nextBoolean()) {
                eventsArray.add(generateNotification());
            }
//        用户后台活跃
            if (rand.nextBoolean()) {
                eventsArray.add(generateBackground());
            }
            json.put("et", eventsArray);
//        时间
            long millis = System.currentTimeMillis();
            logger.info(millis + "|" + json.toJSONString());
        }
    }

    /**
     * 公共字段设置
     *
     * @param uid_length
     * @return
     */
    static JSONObject generateComFields(int uid_length) {
//		公共字段
        JSONObject common = new JSONObject();
//		用户id
        common.put("uid", 'u' + getRandomDigits(uid_length));
//		程序版本号 5,6等
        common.put("vc", "" + (rand.nextInt(20)));
//		程序版本名 v1.1.1
        common.put("vn", "1." + rand.nextInt(4) + "." + rand.nextInt(10));
//        安卓系统版本
        common.put("os","8."+rand.nextInt(3)+"."+rand.nextInt(10));
//		语言  es,en,pt
        int flag = rand.nextInt(3);
        switch (flag) {
            case (0):
                common.put("l", "es");
                break;
            case (1):
                common.put("l", "en");
                break;
            case (2):
                common.put("l", "pt");
                break;
        }
//  渠道号   从哪个渠道来的
        common.put("sr", getRandomChar(1));
//        区域
        flag = rand.nextInt(2);
        switch (flag) {
            case 0:
                common.put("ar", "BR");
            case 1:
                common.put("ar", "MX");
        }
//        手机品牌 ba ,手机型号 md，就取2位数字了
        flag = rand.nextInt(3);
        switch (flag) {
            case 0:
                common.put("ba", "Sumsung");
                common.put("md", "sumsung-" + rand.nextInt(20));
                break;
            case 1:
                common.put("ba", "Huawei");
                common.put("md", "huawei-" + rand.nextInt(20));
                break;
            case 2:
                common.put("ba", "HTC");
                common.put("md", "htc-" + rand.nextInt(20));
                break;
        }
//    嵌入sdk 的版本
        common.put("sv", "V2." + rand.nextInt(10) + "." + rand.nextInt(10));
//    gmail
        common.put("g", getRandomCharAndNumr(8) + "@gmail.com");
//        屏幕宽高 hw
        flag = rand.nextInt(4);
        switch (flag) {
            case 0:
                common.put("hw", "640*960");
                break;
            case 1:
                common.put("hw", "640*1136");
                break;
            case 2:
                common.put("hw", "750*1134");
                break;
            case 3:
                common.put("hw", "1080*1920");
                break;
        }
//        客户端产生日志时间
        long millis = System.currentTimeMillis();
        common.put("t", "" + (millis - rand.nextInt(99999999)));
//		手机网络模式 3G,4G,WIFI
        flag = rand.nextInt(3);
        switch (flag) {
            case 0:
                common.put("nw", "3G");
                break;
            case 1:
                common.put("nw", "4G");
                break;
            case 2:
                common.put("nw", "WIFI");
                break;
        }
//       拉丁美洲 西经34°46′至西经117°09；北纬32°42′至南纬53°54′
//       经度
        common.put("ln", (-34 - rand.nextInt(83)-rand.nextInt(60)/10.0) + "");
//        纬度
        common.put("la", (32 - rand.nextInt(85)-rand.nextInt(60)/10.0) + "");
        return common;
    }

    /**
     * 新闻展示事件
     *
     * @return
     */
    static JSONObject generateDisplay(int newsid_length) {
        JSONObject jsonObject = new JSONObject();
        boolean boolFlag = rand.nextInt(10) < 7 ? true : false;
//	    动作：曝光新闻=1，点击新闻=2，
        if (boolFlag) {
            jsonObject.put("action", "1");
        } else {
            jsonObject.put("action", "2");
        }
//        新闻id
        String newsId = 'n' + getRandomDigits(newsid_length);
        jsonObject.put("newsid", newsId);
//        顺序  设置成6条吧
        int flag = rand.nextInt(6);
        jsonObject.put("place", "" + flag);
//        新闻来源类型
        flag = 1 + rand.nextInt(3);
        jsonObject.put("showtype", flag + "");
//        版权方  NewYorkTimes,AOL,Yahoo,CNN,ESPN,Reuters
        flag = rand.nextInt(6);
        switch (flag) {
            case 0:
                jsonObject.put("copyright", "NewYorkTimes");
                break;
            case 1:
                jsonObject.put("copyright", "AOL");
                break;
            case 2:
                jsonObject.put("copyright", "Yahoo");
                break;
            case 3:
                jsonObject.put("copyright", "CNN");
                break;
            case 4:
                jsonObject.put("copyright", "ESPN");
                break;
            case 5:
                jsonObject.put("copyright", "Reuters");
                break;
        }
//        合作方
        flag = rand.nextInt(6);
        switch (flag) {
            case 0:
                jsonObject.put("content_provider", "NewYorkTimes");
                break;
            case 1:
                jsonObject.put("content_provider", "AOL");
                break;
            case 2:
                jsonObject.put("content_provider", "Yahoo");
                break;
            case 3:
                jsonObject.put("content_provider", "CNN");
                break;
            case 4:
                jsonObject.put("content_provider", "ESPN");
                break;
            case 5:
                jsonObject.put("content_provider", "Reuters");
                break;
        }
// 新闻内容类型
        flag = rand.nextInt(10);
        jsonObject.put("newstype", "" + flag);
//    曝光类型
        flag = 1 + rand.nextInt(2);
        jsonObject.put("extend1", "" + flag);
//     新闻样式
        flag = rand.nextInt(6);
        jsonObject.put("extend2", "" + flag);
//        分类
        flag = 1 + rand.nextInt(100);
        jsonObject.put("category", "" + flag);
        return packEventJson("display",jsonObject);
    }

    /**
     * 新闻详情页
     *
     * @param newsid_length
     * @return
     */
    static JSONObject generateNewsDetail(int newsid_length) {
        JSONObject eventJson = new JSONObject();
        int flag = 1 + rand.nextInt(3);
//        页面入口来源
        eventJson.put("entry", flag + "");
//        动作
        eventJson.put("action", "" + (rand.nextInt(4) + 1));
//        新闻id
        eventJson.put("newsid", 'n' + getRandomDigits(newsid_length));
        //        新闻来源类型
        flag = 1 + rand.nextInt(3);
        eventJson.put("showtype", flag + "");
        //        版权方  NewYorkTimes,AOL,Yahoo,CNN,ESPN,Reuters
        flag = rand.nextInt(6);
        switch (flag) {
            case 0:
                eventJson.put("copyright", "NewYorkTimes");
                break;
            case 1:
                eventJson.put("copyright", "AOL");
                break;
            case 2:
                eventJson.put("copyright", "Yahoo");
                break;
            case 3:
                eventJson.put("copyright", "CNN");
                break;
            case 4:
                eventJson.put("copyright", "ESPN");
                break;
            case 5:
                eventJson.put("copyright", "Reuters");
                break;
        }
//        合作方
        flag = rand.nextInt(6);
        switch (flag) {
            case 0:
                eventJson.put("content_provider", "NewYorkTimes");
                break;
            case 1:
                eventJson.put("content_provider", "AOL");
                break;
            case 2:
                eventJson.put("content_provider", "Yahoo");
                break;
            case 3:
                eventJson.put("content_provider", "CNN");
                break;
            case 4:
                eventJson.put("content_provider", "ESPN");
                break;
            case 5:
                eventJson.put("content_provider", "Reuters");
                break;
        }
// 新闻内容类型
        flag = rand.nextInt(10);
        eventJson.put("newstype", "" + flag);
        //     新闻样式
        flag = rand.nextInt(6);
        eventJson.put("show_style", "" + flag);
//        页面停留时长
        flag = rand.nextInt(10) * rand.nextInt(7);
        eventJson.put("news_staytime", flag + "");
//        加载时长
        flag = rand.nextInt(10) * rand.nextInt(7);
        eventJson.put("loading_time", flag + "");
//       加载失败码
        flag = rand.nextInt(10);
        switch (flag) {
            case 1:
                eventJson.put("type1", "102");
                break;
            case 2:
                eventJson.put("type1", "201");
                break;
            case 3:
                eventJson.put("type1", "325");
                break;
            case 4:
                eventJson.put("type1", "433");
                break;
            case 5:
                eventJson.put("type1", "542");
                break;
            default:
                eventJson.put("type1", "");
                break;
        }
        //        分类
        flag = 1 + rand.nextInt(100);
        eventJson.put("category", "" + flag);
//        动作对象
        flag = 1 + rand.nextInt(8);
        eventJson.put("content", "" + flag);
        return packEventJson("newsdetailpro", eventJson);
    }

    /**
     * 新闻列表
     * @return
     */
    static JSONObject generateNewList(){
        JSONObject jsonObject=new JSONObject();
//        动作
        int flag=rand.nextInt(3)+1;
        jsonObject.put("action",flag+"");
//        加载时长
        flag = rand.nextInt(10) * rand.nextInt(7);
        jsonObject.put("loading_time", flag + "");
        //       失败码
        flag = rand.nextInt(10);
        switch (flag) {
            case 1:
                jsonObject.put("type1", "102");
                break;
            case 2:
                jsonObject.put("type1", "201");
                break;
            case 3:
                jsonObject.put("type1", "325");
                break;
            case 4:
                jsonObject.put("type1", "433");
                break;
            case 5:
                jsonObject.put("type1", "542");
                break;
            default:
                jsonObject.put("type1", "");
                break;
        }
        //      页面  加载类型
        flag = 1 + rand.nextInt(2);
        jsonObject.put("loading_way", "" + flag);
//        扩展字段1
        jsonObject.put("extend1", "");
//        扩展字段2
        jsonObject.put("extend2", "");
//        用户加载类型
        flag = 1 + rand.nextInt(3);
        jsonObject.put("type", "" + flag);
        return packEventJson("loading",jsonObject);
    }

    /**
     * 广告相关字段
     * @return
     */
    static JSONObject generateAd(){
        JSONObject jsonObject=new JSONObject();
//        入口
        int flag=rand.nextInt(3)+1;
        jsonObject.put("entry",flag+"");
//        动作
        flag=rand.nextInt(5)+1;
        jsonObject.put("action",flag+"");
//        状态
        flag=rand.nextInt(10)>6?2:1;
        jsonObject.put("content",flag+"");
//        失败码
        flag = rand.nextInt(10);
        switch (flag) {
            case 1:
                jsonObject.put("detail", "102");
                break;
            case 2:
                jsonObject.put("detail", "201");
                break;
            case 3:
                jsonObject.put("detail", "325");
                break;
            case 4:
                jsonObject.put("detail", "433");
                break;
            case 5:
                jsonObject.put("detail", "542");
                break;
            default:
                jsonObject.put("detail", "");
                break;
        }
//        广告来源
        flag=rand.nextInt(4)+1;
        jsonObject.put("source",flag+"");
//        用户行为
        flag=rand.nextInt(2)+1;
        jsonObject.put("behavior",flag+"");
//        新闻类型
        flag = rand.nextInt(10);
        jsonObject.put("newstype", "" + flag);
//        展示样式
        flag = rand.nextInt(6);
        jsonObject.put("show_style", "" + flag);
        return packEventJson("ad",jsonObject);
    }

    /**
     * 启动日志
     * @return
     */
    static JSONObject generateStart(){
        JSONObject jsonObject=new JSONObject();
//        入口
        int flag=rand.nextInt(5)+1;
        jsonObject.put("entry",flag+"");
//        开屏广告类型
        flag=rand.nextInt(2)+1;
        jsonObject.put("open_ad_type",flag+"");
//        状态
        flag=rand.nextInt(10)>8?2:1;
        jsonObject.put("action",flag+"");
//        加载时长
        jsonObject.put("loading_time",rand.nextInt(20)+"");
//        失败码
        flag = rand.nextInt(10);
        switch (flag) {
            case 1:
                jsonObject.put("detail", "102");
                break;
            case 2:
                jsonObject.put("detail", "201");
                break;
            case 3:
                jsonObject.put("detail", "325");
                break;
            case 4:
                jsonObject.put("detail", "433");
                break;
            case 5:
                jsonObject.put("detail", "542");
                break;
            default:
                jsonObject.put("detail", "");
                break;
        }
        return packEventJson("start",jsonObject);
    }

    /**
     * 消息通知
     * @return
     */
    static JSONObject generateNotification(){
        JSONObject jsonObject=new JSONObject();
        int flag=rand.nextInt(4)+1;
//        动作
        jsonObject.put("action",flag+"");
//        通知id
        flag=rand.nextInt(4)+1;
        jsonObject.put("type",flag+"");
//        客户端弹时间
        jsonObject.put("ap_time",(System.currentTimeMillis()-rand.nextInt(99999999))+"");
//        备用字段
        jsonObject.put("content","");
        return  packEventJson("notification",jsonObject);
    }

    /**
     * 后台活跃
     * @return
     */
    static JSONObject generateBackground(){
        JSONObject jsonObject=new JSONObject();
//        启动源
        int flag=rand.nextInt(3)+1;
        jsonObject.put("active_source",flag+"");
        return packEventJson("active_background",jsonObject);
    }



    static JSONObject packEventJson(String eventName, JSONObject jsonObject) {
        JSONObject eventJson = new JSONObject();
        eventJson.put("ett", (System.currentTimeMillis() - rand.nextInt(99999999)) + "");
        eventJson.put("en", eventName);
        eventJson.put("kv", jsonObject);
        return eventJson;
    }

    /**
     * 获取定长度的数字
     *
     * @param leng
     * @return
     */
    static String getRandomDigits(int leng) {
        String result = "";
        for (int i = 0; i < leng; i++) {
            result += rand.nextInt(10);
        }
        return result;
    }

    /**
     * 获取随机字母组合
     *
     * @param length 字符串长度
     * @return
     */
    public static String getRandomChar(Integer length) {
        String str = "";
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            // 字符串
            str += (char) (65 + random.nextInt(26));// 取得大写字母
        }
        return str;
    }

    /**
     * 获取随机字母数字组合
     *
     * @param length 字符串长度
     * @return
     */
    public static String getRandomCharAndNumr(Integer length) {
        String str = "";
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            boolean b = random.nextBoolean();
            if (b) { // 字符串
                // int choice = random.nextBoolean() ? 65 : 97; 取得65大写字母还是97小写字母
                str += (char) (65 + random.nextInt(26));// 取得大写字母
            } else { // 数字
                str += String.valueOf(random.nextInt(10));
            }
        }
        return str;
    }


}
