package com.zhiyi.feature;

import java.io.IOException;
import java.util.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.zhiyi.Config;
import com.zhiyi.Util;

public class FeatureReducer extends ReducerBase {
    private Record result;

    private static int SALE_DATE_INDEX = FeatureMapper.valueList.indexOf("sale_date");
    private static int SALE_QUANTITY_COL = FeatureMapper.valueList.indexOf("sale_quantity");

    @Override
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record keyRecord, Iterator<Record> values, TaskContext context) throws IOException {
        List<List<Long>> list = new ArrayList<List<Long>>();
        while (values.hasNext()) {
            Record valRecord = values.next();

            List<Long> valueList = new ArrayList<Long>();
            for (String key: FeatureMapper.valueList) {
                valueList.add(valRecord.getBigint(key));
            }

            list.add(valueList);
        }

        Collections.sort(list, new Comparator<List<Long>>() {
            public int compare(List<Long> o1, List<Long> o2) {
                return (int) (o1.get(0) - o2.get(0));
            }
        });

        int SIZE = list.get(0).size();
        Double mean = Util.calculateMean(list, SALE_QUANTITY_COL);
        Double std  = Util.calculateStd(list, SALE_QUANTITY_COL, mean);
        for (int row = 0; row < list.size(); row++) {
            Long sale_quantity = list.get(row).get(SALE_QUANTITY_COL);
            Double smooth = Math.min(Math.max(sale_quantity, mean - 3 * std), mean + 3 * std);
            list.get(row).add(smooth.longValue());
        }

        for (int row = 0; row < list.size(); row++) {
            for (String key: Config.KEYS) {
                result.setBigint(key, keyRecord.getBigint(key));
            }

            Long sale_quantity_origin = list.get(row).get(SALE_QUANTITY_COL);
            Long sale_quantity = list.get(row).get(SIZE);
            Long label_smooth = Util.listGet(list, row + Config.PREDICT_GAP, SIZE);
            result.setDouble("label", Util.log1p(label_smooth));

            Long label_origin = Util.listGet(list, row + Config.PREDICT_GAP, SALE_QUANTITY_COL);
            result.setDouble("label_origin", Util.log1p(label_origin));
            result.setBigint("count", (long) list.size());

            for (int i = 0; i < FeatureMapper.valueList.size(); i++) {
                String key = FeatureMapper.valueList.get(i);
                result.setBigint(key, list.get(row).get(i));
            }
            for (int i = 3; i < FeatureMapper.valueList.size(); i++) {
                String key = FeatureMapper.valueList.get(i);
                Double rate = Util.increaseRatio(Math.max(sale_quantity_origin, 0), list.get(row).get(i));
                result.setDouble(key + "_rate", rate);
            }

            Long lastyear_quantity = Util.listGet(list, row - 12 + Config.PREDICT_GAP, SIZE);
            result.setBigint("lastyear_quantity", lastyear_quantity);

            Long lastyearmonth_quantity = Util.listGet(list, row - 12, SIZE);
            result.setBigint("diff_quantity_lastyear", Util.increase(lastyearmonth_quantity, lastyear_quantity));
            result.setDouble("diffratio_quantity_lastyear", Util.increaseRatio(lastyearmonth_quantity, lastyear_quantity));

            Long lastmonth_quantity = Util.listGet(list, row - 1, SIZE);
            result.setBigint("diff_quantity", Util.increase(lastmonth_quantity, sale_quantity));
            result.setDouble("diffratio_quantity", Util.increaseRatio(lastmonth_quantity, sale_quantity));

            for (int w: Config.windows) {
                Double rmean = Util.rollingMean(list, row, SIZE, w);
                result.setDouble("mean_quantity_" + w, rmean);
            }

            Double diff_quantity_12 = Util.increase(lastyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 12));
            Double diff_quantity_24 = Util.increase(lastyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 24));
            Double diffratio_quantity_12 = Util.increaseRatio(lastyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 12));
            Double diffratio_quantity_24 = Util.increaseRatio(lastyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 24));
            result.setDouble("diff_quantity_12", diff_quantity_12);
            result.setDouble("diff_quantity_24", diff_quantity_24);
            result.setDouble("diffratio_quantity_12", diffratio_quantity_12);
            result.setDouble("diffratio_quantity_24", diffratio_quantity_24);

            Long prevyear_quantity = Util.listGet(list, row - 24 + Config.PREDICT_GAP, SIZE);
            Double diff_prevyear_quantity_12 = Util.increase(prevyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 12));
            Double diff_prevyear_quantity_24 = Util.increase(prevyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 24));
            Double diffratio_prevyear_quantity_12 = Util.increaseRatio(prevyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 12));
            Double diffratio_prevyear_quantity_24 = Util.increaseRatio(prevyear_quantity.doubleValue(), result.getDouble("mean_quantity_" + 24));
            result.setBigint("prevyear_quantity", prevyear_quantity);
            result.setDouble("diff_prevyear_quantity_12", diff_prevyear_quantity_12);
            result.setDouble("diff_prevyear_quantity_24", diff_prevyear_quantity_24);
            result.setDouble("diffratio_prevyear_quantity_12", diffratio_prevyear_quantity_12);
            result.setDouble("diffratio_prevyear_quantity_24", diffratio_prevyear_quantity_24);

            for (int i = 2; i < Config.windows.length; i++) {
                int window = Config.windows[i];
                Long rmedian = Util.rollingMedian(list, row, SIZE, window);
                Long rmax = Util.rollingMax(list, row, SIZE, window);
                Long rmin = Util.rollingMin(list, row, SIZE, window);
                result.setBigint("median_quantity_" + window, rmedian);
                result.setBigint("max_quantity_" + window, rmax);
                result.setBigint("min_quantity_" + window, rmin);
            }

            for (int i = 1; i < Config.windows.length; i++) {
                int prev = Config.windows[i-1];
                int cur = Config.windows[i];
                Double prev_mean = result.getDouble("mean_quantity_" + prev);
                Double cur_mean = result.getDouble("mean_quantity_" + cur);
                result.setDouble("inc_quantity_" + prev + '_' + cur, Util.increaseRatio(prev_mean, cur_mean));
            }

            Long sale_date = list.get(row).get(SALE_DATE_INDEX);
            Long year = sale_date / 100L;
            Long month = sale_date % 100L;

            result.setBigint("year", year);
            result.setBigint("month", month);
            Util.onehot(result, "month_", month, 12);

            Long province_id = keyRecord.getBigint("province_id");
            Util.onehot(result, "province_", province_id, 31);

            // 是否为春节
            long is_spring_festival = 0L;
            if (Config.SPRING_FESTIVAL.contains(sale_date)) {
                is_spring_festival = 1L;
            }
            result.setBigint("is_spring_festival", is_spring_festival);

            // 春节前一个月
            long before_spring_festival = 0L;
            if (Config.BEFORE_SPRING_FESTIVAL.contains(sale_date)) {
                before_spring_festival = 1L;
            }
            result.setBigint("before_spring_festival", before_spring_festival);

            // 春节后一个月
            long after_spring_festival = 0L;
            if (Config.AFTER_SPRING_FESTIVAL.contains(sale_date)) {
                after_spring_festival = 1L;
            }
            result.setBigint("after_spring_festival", after_spring_festival);

            context.write(result);
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}