package com.tom.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 验证用的
 */
public class LogValidateInterceptor implements Interceptor {
    public void initialize() {
    }


    /**
     * 有些日志是多行的这种情况，需要将多行拆开成一条一条的日志。
     * @param event
     * @return
     */
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        // body为原始数据，newBody为处理后的数据,判断是否为display的数据类型
        if (LogUtils.validateReportLog(body)) {
            return event;
        }
        return null;
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
            return new LogValidateInterceptor();
        }

        public void configure(Context context) {

        }
    }

}

