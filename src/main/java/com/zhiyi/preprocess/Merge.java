package com.zhiyi.preprocess;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class Merge {
    private static final String KEY_SCHEMA = "province_id:bigint,city_id:bigint,class_id:bigint,sale_date:string";
    private static final String VALUE_SCHEMA = "sale_quantity:bigint,brand_id:bigint,emission_standards_id:bigint,if_luxurious_id:bigint,if_mpv_id:bigint,level_id:bigint,tr:string,gearbox_type:string,price_level:string,rated_passenger:string";
    public static void main(String[] args) throws OdpsException {

        JobConf job = new JobConf();

        // TODO: specify map output types
        job.setMapOutputKeySchema(SchemaUtils.fromString(KEY_SCHEMA));
        job.setMapOutputValueSchema(SchemaUtils.fromString(VALUE_SCHEMA));

        // TODO: specify input and output tables
        InputUtils.addTable(TableInfo.builder().tableName("yc_passenger_car_sales").build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("car_sales_merged").build(), job);

        // TODO: specify a mapper
        job.setMapperClass(MergeMapper.class);
        // TODO: specify a reducer
        job.setReducerClass(MergeReducer.class);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();

    }

}