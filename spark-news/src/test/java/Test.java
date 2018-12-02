import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Test {
    public static void main(String[] args) throws IOException {
        try {
        BufferedReader br = new BufferedReader(new InputStreamReader( System.in ) ) ;  //java.io.InputStreamReader继承了Reader类
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("HH:mm");
        String tx = null;
        List<String> dats=new ArrayList<>();
        while(StringUtils.isNoneBlank(tx=br.readLine( ))){
            String[] arr=tx.split("\t");
            String[] tims=arr[arr.length-1].split(",");
                Date morning=simpleDateFormat.parse(tims[0]);
                Date afternoon=simpleDateFormat.parse(tims[tims.length-1]);
                if(afternoon.getTime()-morning.getTime()<9*60*60*1000){
                    dats.add(tx);
                }
        }

        dats.forEach(x->{
            String[] arr=x.split("\t");
            String[] tims=arr[arr.length-1].split(",");
            try {
                Date morning=simpleDateFormat.parse(tims[0]);
                Calendar calendar=Calendar.getInstance();
                calendar.setTimeInMillis(morning.getTime());
                calendar.add(Calendar.HOUR_OF_DAY,9);
                System.out.println(arr[arr.length-2]+" "+simpleDateFormat.format(calendar.getTime()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        });
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}

