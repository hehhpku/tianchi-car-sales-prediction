package com.zhiyi.feature;

import java.io.IOException;
import java.util.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.zhiyi.Config;
import com.zhiyi.Util;

public class FillReducer extends ReducerBase {
    private Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record keyRecord, Iterator<Record> values, TaskContext context) throws IOException {
        Map<Long, List<Long>> map = new HashMap<Long, List<Long>>();
        while (values.hasNext()) {
            Record valRecord = values.next();
            List<Long> valueList = new ArrayList<Long>();
            Long sale_date = valRecord.getBigint("sale_date");
            for (String key: FillMapper.valueList) {
                valueList.add(valRecord.getBigint(key));
            }
            map.put(sale_date, valueList);
        }

        // 填充缺失值
        Long firstMonth = Collections.min(map.keySet());    // 获取有值的最小月
        int index = 0;
        int pos = Config.SALE_DATE.indexOf(firstMonth);     // 找出最小月之后的所有月份，挨个排查是否有缺失值
        for (int i = pos; i < Config.SALE_DATE.size(); i++) {
            for (String key: Config.KEYS) {
                result.setBigint(key, keyRecord.getBigint(key));
            }
            result.setBigint("index", (long) index);

            Long month = Config.SALE_DATE.get(i);
            result.setBigint("sale_date", month);

            if (map.containsKey(month)) {   // 如果没有缺失
                for (int j = 1; j < FillMapper.valueList.size(); j++) {
                    String key = FillMapper.valueList.get(j);
                    Long value = map.get(month).get(j);
                    result.setBigint(key, value);
                }
            } else {                        // 如果有缺失
                for (int j = 1; j < FillMapper.valueList.size(); j++) {
                    String key = FillMapper.valueList.get(j);
                    result.setBigint(key, 0L);
                }
            }

            // 写数据
            context.write(result);
            index++;
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}