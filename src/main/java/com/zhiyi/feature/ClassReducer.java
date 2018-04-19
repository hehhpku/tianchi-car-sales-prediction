package com.zhiyi.feature;

import java.io.IOException;
import java.util.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.zhiyi.Config;
import com.zhiyi.Util;

public class ClassReducer extends ReducerBase {
    private Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record keyRecord, Iterator<Record> values, TaskContext context) throws IOException {
        List<List<Long>> list = new ArrayList<List<Long>>();
        while (values.hasNext()) {
            Record valRecord = values.next();
            List<Long> valueList = new ArrayList<Long>(2);
            valueList.add(valRecord.getBigint("produce_date"));
            valueList.add(valRecord.getBigint("produce_quantity"));

            list.add(valueList);
        }

        Collections.sort(list, new Comparator<List<Long>>() {
            public int compare(List<Long> o1, List<Long> o2) {
                return (int) (o1.get(0) - o2.get(0));
            }
        });

        for (int row = 0; row < list.size(); row++) {
            Long produce_date = list.get(row).get(0);
            Long produce_quantity = list.get(row).get(1);

            result.setBigint("class_id", keyRecord.getBigint("class_id"));
            result.setBigint("produce_date", produce_date);
            result.setBigint("produce_quantity", produce_quantity);

            for (int window: Config.windows) {
                Double mean = Util.rollingMean(list, row, 1, window);
                result.setDouble("mean_produce_quantity_" + window, mean);
            }
            for (int window: Config.windows) {
                if (window >= 3) {
                    Long median = Util.rollingMedian(list, row, 1, window);
                    Long max = Util.rollingMax(list, row, 1, window);
                    Long min = Util.rollingMin(list, row, 1, window);
                    result.setBigint("median_produce_quantity_" + window, median);
                    result.setBigint("max_produce_quantity_" + window, max);
                    result.setBigint("min_produce_quantity_" + window, min);
                }
            }
            context.write(result);
        }

    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        sb.append("class_id:bigint,");
        sb.append("produce_date:bigint,");
        sb.append("produce_quantity:bigint,");

        for (int window: Config.windows) {
            sb.append("mean_produce_quantity_" + window + ":double,");
        }
        for (int window: Config.windows) {
            if (window >= 3) {
                sb.append("median_produce_quantity_" + window + ":bigint,");
                sb.append("max_produce_quantity_" + window + ":bigint,");
                sb.append("min_produce_quantity_" + window + ":bigint,");
            }
        }
        System.out.println(sb.toString());
    }
}