import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;
import java.util.*;


public class Test {
    public static void main(String[] args) throws IOException {
        String val="1539354173597|{\"appkey\":\"browser\", \"he\": {\"userId\": \"u1252\", \"area\": \"BR\", \"appVersion\": \"V1.0.2\",\"appTime\": \"1539423902511\"}, \"et\": [{\"eventName\": \"display\", \"kv\": {\"action\": \"1\",\"newsId\": \"n4841\"}},'           '{\"eventName\": \"background\",\"kv\": {\"createTime\": \"1539415690455\"}},{\"eventName\": \"foreground\",\"kv\": {\"createTime\": \"1539423902511\"}}]}";
        ObjectMapper objectMapper=new ObjectMapper();
        String timestam=val.split("\\|")[0];
        JsonNode jsonNode=objectMapper.readTree(val.split("\\|")[1]);
//            用户id，news id，area，时间
        String userid=jsonNode.get("he").get("userId").asText();
        String area=jsonNode.get("he").get("area").asText();
        Iterator<JsonNode> iterater=jsonNode.get("et").iterator();
        String newsId=null;
        while(iterater.hasNext()){
            JsonNode event=iterater.next();
            if("display".equals(event.get("eventName").asText())){
                int action=event.get("kv").get("action").asInt();
                newsId=event.get("kv").get("newsId").asText();
                System.out.println(timestam+userid+area+newsId+" "+action);
            }
        }
    }
}

