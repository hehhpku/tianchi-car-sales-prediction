package com.zhiyi;

import org.apache.commons.collections.MapUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {
    public static final String SEPARATOR = "_";
    public static final String SALE_QUANTITY = "sale_quantity";
    public static final List<String> KEYS = Arrays.asList("province_id", "city_id", "class_id");

    // 预测未来第几个月
    public static final int PREDICT_GAP = 2;

    // 滑动窗口
    public static final int[] windows = {2, 3, 6, 12, 24};

    public static final List<Long> SPRING_FESTIVAL = Arrays.asList(
            201212L, 201312L, 201412L,
            201512L, 201611L, 201712L
    );
    public static final List<Long> BEFORE_SPRING_FESTIVAL = Arrays.asList(
            201211L, 201311L, 201411L,
            201511L, 201610L, 201711L
    );
    public static final List<Long> AFTER_SPRING_FESTIVAL = Arrays.asList(
            201301L, 201401L, 201501L,
            201601L, 201612L, 201801L
    );

    public static String[] merge_key = {"brand_id", "department_id",
            "emission_standards_id", "if_luxurious_id", "if_mpv_id", "level_id",
            "tr", "gearbox_type", "price_level", "rated_passenger"
    };

    public static List<Long> SALE_DATE = Arrays.asList(
            201201L, 201202L, 201203L, 201204L, 201205L, 201206L, 201207L, 201208L, 201209L, 201210L, 201211L, 201212L,
            201301L, 201302L, 201303L, 201304L, 201305L, 201306L, 201307L, 201308L, 201309L, 201310L, 201311L, 201312L,
            201401L, 201402L, 201403L, 201404L, 201405L, 201406L, 201407L, 201408L, 201409L, 201410L, 201411L, 201412L,
            201501L, 201502L, 201503L, 201504L, 201505L, 201506L, 201507L, 201508L, 201509L, 201510L, 201511L, 201512L,
            201601L, 201602L, 201603L, 201604L, 201605L, 201606L, 201607L, 201608L, 201609L, 201610L, 201611L, 201612L,
            201701L, 201702L, 201703L, 201704L, 201705L, 201706L, 201707L, 201708L, 201709L, 201710L, 201711L, 201712L
    );
    public static Map<Long, Integer> DATE_INDEX_MAP = new HashMap<Long, Integer>();

    public static Map<String, List<Integer>> intMap = new HashMap<String, List<Integer>>();
    public static Map<String, List<String>> strMap = new HashMap<String, List<String>>();
    static {
        intMap.put("brand_id", Arrays.asList(25, 15, 35, 37, 53, 18, 42, 16, 17, 8, 29, 11, 56, 10, 14, 43, 31, 23));
        intMap.put("emission_standards_id", Arrays.asList(2, 3));
        intMap.put("if_luxurious_id", Arrays.asList(0, 1));
        intMap.put("if_mpv_id", Arrays.asList(0, 1));
        intMap.put("level_id", Arrays.asList(1, 5, 4, 2, 3, 6, 7));

        strMap.put("tr", Arrays.asList("5", "6", "0", "7", "4", "8"));
        strMap.put("gearbox_type", Arrays.asList("MT", "AT", "CVT", "DCT", "AMT"));
        strMap.put("price_level", Arrays.asList("10-15", "5-8", "8-10", "15-20", "20-25", "25-35", "5以下", "35-50"));
        strMap.put("rated_passenger", Arrays.asList("5", "7", "7-8", "5-7", "5-8", "4-5", "6-8", "6-7"));

        for (int i = 0; i < SALE_DATE.size(); i++) {
            DATE_INDEX_MAP.put(SALE_DATE.get(i), i);
        }
    }

    public static void main(String[] args) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<String, List<Integer>> entry : Config.intMap.entrySet()) {
            for (Integer value : entry.getValue()) {
                String key = entry.getKey() + '-' + String.valueOf(value);
                map.put(key, 0);
            }
        }
        for (Map.Entry<String, List<String>> entry : Config.strMap.entrySet()) {
            for (String value : entry.getValue()) {
                String key = entry.getKey() + '-' + String.valueOf(value);
                map.put(key, 0);
            }
        }
        System.out.println(map);
    }
}
