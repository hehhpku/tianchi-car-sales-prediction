package com.zhiyi.feature;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class ClassMapper extends MapperBase {
    Record keyRecord;
    Record valueRecord;

    @Override
    public void setup(TaskContext context) throws IOException {
        keyRecord = context.createMapOutputKeyRecord();
        valueRecord = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        // Key
        keyRecord.setBigint("class_id", record.getBigint("class_id"));

        // Value
        valueRecord.setBigint("produce_date", Long.parseLong(record.getString("produce_date")));
        valueRecord.setBigint("produce_quantity", record.getBigint("produce_quantity"));

        // Context
        context.write(keyRecord, valueRecord);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}