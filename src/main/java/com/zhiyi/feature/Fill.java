package com.zhiyi.feature;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class Fill {
    public static final String KEY_SCHEMA = "province_id:bigint,city_id:bigint,class_id:bigint";
    public static final String VALUE_SCHEMA = "sale_date:bigint,sale_quantity:bigint,tr_7:bigint,tr_8:bigint,tr_5:bigint,tr_6:bigint,tr_4:bigint,rated_passenger_4_5:bigint,rated_passenger_7:bigint,rated_passenger_5:bigint,price_level_5less:bigint,price_level_15_20:bigint,price_level_5_8:bigint,brand_id_42:bigint,brand_id_43:bigint,tr_0:bigint,if_luxurious_id_1:bigint,if_luxurious_id_0:bigint,brand_id_37:bigint,price_level_10_15:bigint,brand_id_8:bigint,emission_standards_id_2:bigint,if_mpv_id_1:bigint,if_mpv_id_0:bigint,price_level_25_35:bigint,emission_standards_id_3:bigint,level_id_3:bigint,brand_id_31:bigint,rated_passenger_7_8:bigint,level_id_4:bigint,level_id_1:bigint,rated_passenger_5_8:bigint,level_id_2:bigint,level_id_7:bigint,brand_id_35:bigint,level_id_5:bigint,level_id_6:bigint,price_level_8_10:bigint,brand_id_29:bigint,rated_passenger_5_7:bigint,gearbox_type_AT:bigint,gearbox_type_MT:bigint,price_level_20_25:bigint,price_level_35_50:bigint,brand_id_25:bigint,brand_id_23:bigint,brand_id_17:bigint,brand_id_18:bigint,brand_id_15:bigint,brand_id_16:bigint,gearbox_type_DCT:bigint,gearbox_type_CVT:bigint,gearbox_type_AMT:bigint,brand_id_53:bigint,brand_id_10:bigint,rated_passenger_6_7:bigint,rated_passenger_6_8:bigint,brand_id_14:bigint,brand_id_11:bigint,brand_id_56:bigint";

    public static void main(String[] args) throws OdpsException {

        JobConf job = new JobConf();

        // TODO: specify map output types
        job.setMapOutputKeySchema(SchemaUtils.fromString(KEY_SCHEMA));
        job.setMapOutputValueSchema(SchemaUtils.fromString(VALUE_SCHEMA));

        // TODO: specify input and output tables
        InputUtils.addTable(TableInfo.builder().tableName("car_sales_merged").build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("car_sales_filled").build(), job);

        // TODO: specify a mapper
        job.setMapperClass(FillMapper.class);
        // TODO: specify a reducer
        job.setReducerClass(FillReducer.class);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();

    }

}