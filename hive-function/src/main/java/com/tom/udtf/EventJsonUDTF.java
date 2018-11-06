package com.tom.udtf;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

/**
 * 用于将json串中的json数组展平，将eventname字段提取出来，并且保存成event_name 和event_json
 *
 * @author tomluo
 *
 */
public class EventJsonUDTF extends GenericUDTF {

        @Override
        public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
            ArrayList<String> fieldNames = new ArrayList<>();
            ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
            fieldNames.add("event_name");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            fieldNames.add("event_json");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        @Override
        public void process(Object[] objects) throws HiveException {
            String input = objects[0].toString();
            if ("".equals(input) || input == null) {
                return;
            }
            JSONArray ja = null;
            if (StringUtils.isNotBlank(input)) {
                try {
                    ja = new JSONArray(input);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                if (ja == null)
                    return;
                for (int i = 0; i < ja.length(); i++) {
                    String[] result = new String[2];
                    try {
                        result[0] = ja.getJSONObject(i).getString("en");
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    forward(result);
                }
            }
        }

        @Override
        public void close() throws HiveException {

        }

    }

