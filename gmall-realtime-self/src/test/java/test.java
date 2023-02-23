import com.alibaba.fastjson.JSONObject;
import org.apache.commons.net.ntp.TimeStamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[] args) throws ParseException {
        String dateTime = "2022-02-21 12:58:29";

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date curDate = simpleDateFormat.parse(dateTime);
        TimeStamp timeStamp = new TimeStamp(curDate);
        long time = timeStamp.getTime();

        System.out.println(time);

        String stamp = String.valueOf(time);


        String jsonText = "{\"id\":\"17935\",\"order_id\":\"6602\",\"sku_id\":\"9\",\"sku_name\":\"Apple iPhone 12 (A2404) 64GB 红色 支持移动联通电信5G 双卡双待手机\",\"order_price\":\"8197.0\",\"sku_num\":\"3\",\"create_time\":"+stamp+",\"source_type\":\"2404\",\"source_id\":\"2\",\"split_total_amount\":\"24091.0\",\"split_activity_amount\":\"500.0\",\"split_coupon_amount\":null,\"consignee\":\"葛以建\",\"consignee_tel\":\"13210836651\",\"total_amount\":\"60950.0\",\"order_status\":\"1001\",\"user_id\":\"2130\",\"delivery_address\":\"第9大街第32号楼4单元152门\",\"order_comment\":\"描述887556\",\"out_trade_no\":\"957478498931255\",\"trade_body\":\"金沙河面条 银丝挂面900g*3包 爽滑 细面条 龙须面 速食面等9件商品\",\"process_status\":null,\"tracking_no\":null,\"parent_order_id\":null,\"province_id\":\"19\",\"activity_reduce_amount\":\"500.0\",\"coupon_reduce_amount\":\"0.0\",\"original_total_amount\":\"61433.0\",\"feight_fee\":\"17.0\",\"feight_fee_reduce\":null,\"refundable_time\":null,\"activity_id\":\"2\",\"activity_rule_id\":\"4\",\"coupon_id\":null,\"coupon_use_id\":null,\"dic_name\":\"未支付\"}\n";


        System.out.println("jsonText"+jsonText);


        JSONObject jsonObject = JSONObject.parseObject(jsonText);
        System.out.println("josnobject"+jsonObject);


    }



}
