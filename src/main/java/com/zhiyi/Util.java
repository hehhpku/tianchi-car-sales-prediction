package com.zhiyi;

import com.aliyun.odps.data.Record;
import com.zhiyi.feature.FeatureMapper;

import java.util.*;

public class Util {

    public static final double PRECISION = 1e8;
    public static final double LOG2 = Math.log(2);

    public static void updateMap(Map<String, Integer> map, String key, int value) {
        if (map.containsKey(key)) {
            int sum = map.get(key);
            sum += value;
            map.put(key, sum);
        }
    }

    public static List<Long> initList(int size, Long sale_date) {
        List<Long> list = new ArrayList<Long>();
        list.add(sale_date);
        for (int i = 1; i < size; i++) {
            list.add(0L);
        }
        return list;
    }

    public static <T, S> S mapGet(Map<T, S> map, T key, S value) {
        if (map.containsKey(key)) {
            return map.get(key);
        }
        return value;
    }

    /**
     *
     * @param list 二维矩阵
     * @param row 矩阵第row行
     * @param col 矩阵第col列
     * @return
     */
    public static Long listGet(List<List<Long>> list, int row, int col) {
        if (row >= 0 && row < list.size()) {
            return list.get(row).get(col);
        }
        return 0L;
    }

    public static List<Long> rolling(List<List<Long>> list, int row, int col, int window) {
        List<Long> valueList = new ArrayList<Long>();
        for (int i = 0; i < window; i++) {
            Long value = listGet(list, row - i, col);
            if (value != null) {
                valueList.add(value);
            }
        }
        return valueList;
    }

    /**
     * 对列表list的第row行第col列求滑动窗口平均值
     * @param list
     * @param row
     * @param col
     * @param window  滑动窗口大小
     * @return
     */
    public static Double rollingMean(List<List<Long>> list, int row, int col, int window) {
        Long sum = 0L;
        List<Long> valueList = rolling(list, row, col, window);
        for (Long value: valueList) {
            sum += value;
        }

        if (valueList.size() <= 0) {
            return 0.0;
        }
        return sum.doubleValue() / valueList.size();
    }

    public static Long rollingMedian(List<List<Long>> list, int row, int col, int window) {
        List<Long> valueList = rolling(list, row, col, window);
        return valueList.get(valueList.size() / 2);
    }

    public static Long rollingMax(List<List<Long>> list, int row, int col, int window) {
        List<Long> valueList = rolling(list, row, col, window);
        return Collections.max(valueList);
    }

    public static Long rollingMin(List<List<Long>> list, int row, int col, int window) {
        List<Long> valueList = rolling(list, row, col, window);
        return Collections.min(valueList);
    }

    public static Double calculateMean(List<List<Long>> list, int col) {
        Double sum = 0d;
        for (int i = 0; i < list.size(); i++) {
            Long value = listGet(list, i, col);
            if (value != null) {
                sum += value.doubleValue();
            }
        }
        return sum / list.size();
    }

    public static Double calculateStd(List<List<Long>> list, int col, double mean) {
        Double redisual = 0d;
        for (int i = 0; i < list.size(); i++) {
            Long value = listGet(list, i, col);
            if (value != null) {
                redisual += Math.pow(value.doubleValue() - mean, 2);
            }
        }
        return Math.sqrt(redisual / list.size());
    }

    public static Double round(Double x) {
        if (x == null) {
            return 0d;
        }
        return Math.round(PRECISION * x) / PRECISION;
    }

    public static Double log1p(Long x) {
        if (x == null) {
            return 0d;
        }
        return round(Math.log(1+ x) / LOG2);
    }

    public static Double increaseRatio(Long a, Long b) {
        if (b == null) {
            return 0d;
        }
        if (a == null || a.intValue() == 0 ) {
            return b.doubleValue();
        }
        return round(b.doubleValue() / a.doubleValue());
    }

    public static Double increaseRatio(Double a, Double b) {
        if (b == null) {
            return 0d;
        }
        if (a == null || a.intValue() == 0 ) {
            return b.doubleValue();
        }
        return round(b.doubleValue() / a.doubleValue());
    }

    public static Long increase(Long a, Long b) {
        if (a == null || b == null) {
            return 0L;
        }
        return b - a;
    }

    public static Double increase(Double a, Double b) {
        if (a == null || b == null) {
            return 0d;
        }
        return b - a;
    }

    public static void onehot(Record record, String prefix, Long value, int size) {
        for (int i = 1; i <= size; i++) {
            if (value.intValue() == i) {
                record.setBigint(prefix + i, 1L);
            }
            else {
                record.setBigint(prefix + i, 0L);
            }
        }
    }

    public static void main(String[] args) {
//        Map<String, Integer> map = new HashMap<String, Integer>();
//        map.put("a", 1);
//        map.put("b", 2);
//        updateMap(map, "a", 10);
//        System.out.println(map);
//        updateMap(map, "b", -1);
//        System.out.println(map);

//        initMap();
//        StringBuilder sb = new StringBuilder();
//        for (Map.Entry<String, Integer> entry: map.entrySet()) {
//            sb.append(entry.getKey().replace("-", Config.SEPARATOR).replace("以下", "less"));
//            sb.append(":BIGINT,");
//        }
//        System.out.println(map.size());
//        System.out.println(sb);

//        List<String> keyList = Arrays.asList("province_id", "city_id", "class_id", "sale_date");
//        if (keyList.contains("city_id")) {
//            System.out.println("contains");
//        } else {
//            System.out.println("not contains");
//        }
//
//        for (String key: FeatureEngineer.VALUE_SCHEMA.split(",")) {
//            String k = key.replace(":bigint", "");
//            if (!k.startsWith("sale_date")) {
//                System.out.println(k);
//            }
//        }
//        System.out.println(log1p(3L));
//        System.out.println(log1p(7L));
//
//        System.out.println(round(1.32323211));
//        System.out.println(round(2.3212512));
//        System.out.println(round(3.12139121));
//
//        List<List<Long>> list = new ArrayList<List<Long>>(12);
//        System.out.println(list.size());

//        for (int i = 3; i < FeatureMapper.valueList.size(); i++) {
//            String key = FeatureMapper.valueList.get(i);
//            sb.append(key + "_rate:double,");
//        }
//        for (int i = 2; i < Config.windows.length; i++) {
//            int window = Config.windows[i];
//            sb.append("median_quantity_" + window + ":bigint,");
//            sb.append("max_quantity_" + window + ":bigint,");
//            sb.append("min_quantity_" + window + ":bigint,");
//        }
//        System.out.println(sb.toString());
    }
}
