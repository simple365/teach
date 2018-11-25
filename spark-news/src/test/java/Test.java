import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;
import java.util.*;


public class Test {
    public static void main(String[] args) throws IOException {
        List<String> res=new ArrayList<>();
        res.add("sfdsd");
        res.add("6f43");
        res.add("345");
        res.forEach(row->{
            row.substring(2);
        });
        res.forEach(row ->{
            System.out.println(row);
        });
    }
}

