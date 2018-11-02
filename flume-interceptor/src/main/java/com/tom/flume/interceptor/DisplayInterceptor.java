package com.tom.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class DisplayInterceptor implements Interceptor {
    public void initialize() {
    }


    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        // body为原始数据，newBody为处理后的数据,判断是否为display的数据类型
        if (body.contains("\"display\"")) {
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
            return new DisplayInterceptor();
        }

        public void configure(Context context) {

        }
    }

}
