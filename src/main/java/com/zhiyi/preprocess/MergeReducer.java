package com.zhiyi.preprocess;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.zhiyi.Config;
import com.zhiyi.Util;

public class MergeReducer extends ReducerBase {
    private Record result;
    private static Map<String, Integer> map = new HashMap<String, Integer>();

    private void initMap() {
        for (Map.Entry<String, List<Integer>> entry : Config.intMap.entrySet()) {
            for (Integer value : entry.getValue()) {
                String key = entry.getKey() + Config.SEPARATOR + String.valueOf(value);
                map.put(key, 0);
            }
        }
        for (Map.Entry<String, List<String>> entry : Config.strMap.entrySet()) {
            for (String value : entry.getValue()) {
                String key = entry.getKey() + Config.SEPARATOR + String.valueOf(value);
                map.put(key, 0);
            }
        }
        map.put("sale_quantity", 0);
    }

    @Override
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record keyrecord, Iterator<Record> values, TaskContext context) throws IOException {
        this.initMap();
        while (values.hasNext()) {
            Record val = values.next();
            int sale_quantity = val.getBigint("sale_quantity").intValue();
            Util.updateMap(map, Config.SALE_QUANTITY, sale_quantity);

            for (String key: Config.intMap.keySet()) {
                Long value = val.getBigint(key);
                String kv = key + Config.SEPARATOR + String.valueOf(value);
                Util.updateMap(map, kv, sale_quantity);
            }

            for (String key: Config.strMap.keySet()) {
                String value = val.getString(key);
                String kv = key + Config.SEPARATOR + String.valueOf(value);
                Util.updateMap(map, kv, sale_quantity);
            }
        }

        String sale_date = keyrecord.getString("sale_date");
        result.setBigint("sale_date", Long.parseLong(sale_date));
        for (String key: Config.KEYS) {
            result.setBigint(key, keyrecord.getBigint(key));
        }

        for (Map.Entry<String, Integer> entry: map.entrySet()) {
            String key = entry.getKey().replace("-", "_").replace("以下", "less");
            result.setBigint(key, entry.getValue().longValue());
        }
        context.write(result);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}