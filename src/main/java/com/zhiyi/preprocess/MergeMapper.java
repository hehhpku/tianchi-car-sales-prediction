package com.zhiyi.preprocess;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.zhiyi.Config;

public class MergeMapper extends MapperBase {
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
        for (String key: Config.KEYS) {
            keyRecord.setBigint(key, record.getBigint(key));
        }
        keyRecord.setString("sale_date", record.getString("sale_date"));

        // Value
        for (String key: new String[] {"sale_quantity", "brand_id", "emission_standards_id",
                "if_luxurious_id", "if_mpv_id", "level_id"}) {
            valueRecord.setBigint(key, record.getBigint(key));
        }
        for (String key: new String[] {"tr", "gearbox_type", "price_level", "rated_passenger"}) {
            valueRecord.setString(key, record.getString(key));
        }

        // Context
        context.write(keyRecord, valueRecord);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}