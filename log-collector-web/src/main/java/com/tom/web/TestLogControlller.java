package com.tom.web;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * 欧洲日志收集
 */
@Controller
public class TestLogControlller {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

        @RequestMapping(value = "hello")
        @ResponseBody
        public String hello(HttpServletRequest request) {
            try {
                String info=new String(uncompress(request.getInputStream()));
                logger.info(info);
            } catch (IOException e) {
                logger.error("gzip decompressed error",e.getMessage());
            }
            return "how old are you";
        }

    public  byte[] uncompress(InputStream inputStream) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPInputStream ungzip = new GZIPInputStream(inputStream);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }

        return out.toByteArray();
    }
}
