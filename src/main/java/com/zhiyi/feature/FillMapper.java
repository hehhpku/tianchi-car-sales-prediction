package com.zhiyi.feature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.zhiyi.Config;

public class FillMapper extends MapperBase {
    Record keyRecord;
    Record valueRecord;

    public static List<String> valueList = new ArrayList<String>();
    static {
        for (String field: Fill.VALUE_SCHEMA.split(",")) {
            String value = field.replace(":bigint", "");
            valueList.add(value);
        }
    }

    @Override
    public void setup(TaskContext context) throws IOException {
        keyRecord = context.createMapOutputKeyRecord();
        valueRecord = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        // Key
        for (String key: Config.KEYS) {
            keyRecord.setBigint(key, record.getBigint(key));
        }

        // Value
        for (String key: valueList) {
            valueRecord.setBigint(key, record.getBigint(key));
        }

        // Context
        context.write(keyRecord, valueRecord);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}