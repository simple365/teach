package com.tom.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class AppkeyInterceptor implements Interceptor {
    public void initialize() {
    }


    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        // 此时都默认是正确的日志，因为已经处理过了。直接找到  "ap":"xxxx", 直接使用java的原始类型来做，不用第三方的框架，减少内存
        String appkey=body.substring(body.indexOf("\"ap\":"));
        appkey=appkey.substring(appkey.indexOf(":")+1,appkey.indexOf(",")).replace("\"","").trim();
        event.getHeaders().put("appkey",appkey);
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = new ArrayList<>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }


    public void close() {
    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new AppkeyInterceptor();
        }

        public void configure(Context context) {

        }
    }

}