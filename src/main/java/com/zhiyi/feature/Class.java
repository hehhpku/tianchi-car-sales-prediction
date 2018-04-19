package com.zhiyi.feature;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class Class {
    public static final String KEY_SCHEMA = "class_id:bigint";
    public static final String VALUE_SCHEMA = "produce_date:bigint,produce_quantity:bigint";

    public static void main(String[] args) throws OdpsException {

        JobConf job = new JobConf();

        // TODO: specify map output types
        job.setMapOutputKeySchema(SchemaUtils.fromString(KEY_SCHEMA));
        job.setMapOutputValueSchema(SchemaUtils.fromString(VALUE_SCHEMA));

        // TODO: specify input and output tables
        InputUtils.addTable(TableInfo.builder().tableName("yc_passenger_car_yields").build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("car_yields_feature").build(), job);

        // TODO: specify a mapper
        job.setMapperClass(ClassMapper.class);
        // TODO: specify a reducer
        job.setReducerClass(ClassReducer.class);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();

    }

}