set hive.exec.mode.local.auto=true;
use dev_realtime_v1_tianxin_yue;
-- 商品基础信息表
CREATE TABLE ods_product_base (
                                  product_id STRING COMMENT '商品ID',
                                  product_name STRING COMMENT '商品名称',
                                  category_id STRING COMMENT '类目ID',
                                  category_name STRING COMMENT '类目名称',
                                  brand_id STRING COMMENT '品牌ID',
                                  brand_name STRING COMMENT '品牌名称',
                                  create_time TIMESTAMP COMMENT '创建时间',
                                  modify_time TIMESTAMP COMMENT '修改时间',
                                  status TINYINT COMMENT '状态(1-上架,0-下架)',
                                  etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '商品基础信息表'
    PARTITIONED BY (ds STRING COMMENT '日期分区');
-- 商品SKU信息表
CREATE TABLE ods_product_sku (
                                 sku_id STRING COMMENT 'SKU ID',
                                 product_id STRING COMMENT '商品ID',
                                 sku_name STRING COMMENT 'SKU名称',
                                 sku_attr STRING COMMENT 'SKU属性',
                                 price DECIMAL(18,2) COMMENT '价格',
                                 cost DECIMAL(18,2) COMMENT '成本价',
                                 stock INT COMMENT '库存',
                                 etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '商品SKU信息表'
    PARTITIONED BY (ds STRING COMMENT '日期分区');
-- 商品销售事实表
CREATE TABLE ods_product_sales (
                                   order_id STRING COMMENT '订单ID',
                                   product_id STRING COMMENT '商品ID',
                                   sku_id STRING COMMENT 'SKU ID',
                                   user_id STRING COMMENT '用户ID',
                                   sale_price DECIMAL(18,2) COMMENT '销售价格',
                                   quantity INT COMMENT '销售数量',
                                   sale_amount DECIMAL(18,2) COMMENT '销售金额',
                                   discount_amount DECIMAL(18,2) COMMENT '优惠金额',
                                   payment_amount DECIMAL(18,2) COMMENT '实付金额',
                                   order_time TIMESTAMP COMMENT '下单时间',
                                   payment_time TIMESTAMP COMMENT '支付时间',
                                   etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '商品销售事实表'
    PARTITIONED BY (ds STRING COMMENT '日期分区');
-- 商品流量表
CREATE TABLE ods_product_traffic (
                                     product_id STRING COMMENT '商品ID',
                                     sku_id STRING COMMENT 'SKU ID',
                                     user_id STRING COMMENT '用户ID',
                                     channel_id STRING COMMENT '渠道ID',
                                     channel_name STRING COMMENT '渠道名称',
                                     pv INT COMMENT '浏览量',
                                     uv INT COMMENT '访客数',
                                     cart_num INT COMMENT '加购数',
                                     fav_num INT COMMENT '收藏数',
                                     event_time TIMESTAMP COMMENT '事件时间'

) COMMENT '商品流量表'
    PARTITIONED BY (ds STRING COMMENT '日期分区');
CREATE TABLE ods_product_review (
                                    review_id STRING COMMENT '评价ID',
                                    product_id STRING COMMENT '商品ID',
                                    sku_id STRING COMMENT 'SKU ID',
                                    user_id STRING COMMENT '用户ID',
                                    user_type TINYINT COMMENT '用户类型(1-新客,2-老客)',
                                    score TINYINT COMMENT '评分(1-5)',
                                    content STRING COMMENT '评价内容',
                                    review_time TIMESTAMP COMMENT '评价时间'

) COMMENT '商品评价表'
    PARTITIONED BY (ds STRING COMMENT '日期分区');
-- 商品关键词表
CREATE TABLE ods_product_keyword (
                                     product_id STRING COMMENT '商品ID',
                                     keyword STRING COMMENT '关键词',
                                     keyword_type TINYINT COMMENT '关键词类型(1-搜索词,2-品类词,3-长尾词,4-修饰词)',
                                     pv INT COMMENT '曝光量',
                                     click INT COMMENT '点击量',
                                     click_rate DECIMAL(18,4) COMMENT '点击率',
                                     order_num INT COMMENT '订单数',
                                     conversion_rate DECIMAL(18,4) COMMENT '转化率',
                                     stat_date DATE COMMENT '统计日期'

) COMMENT '商品关键词表'
    PARTITIONED BY (dt STRING COMMENT '日期分区');
-- 商品内容分析表
CREATE TABLE ods_product_content (
                                     content_id STRING COMMENT '内容ID',
                                     product_id STRING COMMENT '商品ID',
                                     content_type TINYINT COMMENT '内容类型(1-直播,2-短视频,3-图文)',
                                     title STRING COMMENT '标题',
                                     pv INT COMMENT '浏览量',
                                     uv INT COMMENT '访客数',
                                     click_rate DECIMAL(18,4) COMMENT '点击率',
                                     order_num INT COMMENT '订单数',
                                     conversion_rate DECIMAL(18,4) COMMENT '转化率',
                                     content_time TIMESTAMP COMMENT '内容发布时间'

) COMMENT '商品内容分析表'
    PARTITIONED BY (dt STRING COMMENT '日期分区');

CREATE TABLE ads_product_traffic_source (
                                            product_id STRING COMMENT '商品ID',
                                            channel_id STRING COMMENT '渠道ID',
                                            channel_name STRING COMMENT '渠道名称',
                                            stat_date DATE COMMENT '统计日期',
                                            pv BIGINT COMMENT '浏览量',
                                            uv BIGINT COMMENT '访客数',
                                            order_num BIGINT COMMENT '订单数',
                                            conversion_rate DECIMAL(18,4) COMMENT '转化率',
                                            order_user_num BIGINT COMMENT '下单用户数',
                                            payment_num BIGINT COMMENT '支付件数',
                                            payment_amount DECIMAL(18,2) COMMENT '支付金额',
                                            avg_price DECIMAL(18,2) COMMENT '客单价',
                                            refund_rate DECIMAL(18,4) COMMENT '退款率',
                                            score_avg DECIMAL(18,2) COMMENT '平均评分',
                                            price_star TINYINT COMMENT '价格力星级(1-5)',
                                            review_num BIGINT COMMENT '评价总数',
                                            new_customer_review_num BIGINT COMMENT '新客评价数',
                                            old_customer_review_num BIGINT COMMENT '老客评价数',
                                            positive_review_num BIGINT COMMENT '好评数(4-5星)',
                                            negative_review_num BIGINT COMMENT '差评数(1-2星)',
                                            neutral_review_num BIGINT COMMENT '中评数(3星)',
                                            positive_ratio DECIMAL(18,4) COMMENT '好评率',
                                            negative_ratio DECIMAL(18,4) COMMENT '差评率'

) COMMENT '商品流量来源分析表'
    PARTITIONED BY (ds STRING COMMENT '日期分区');
-- 实现SQL
INSERT OVERWRITE TABLE ads_product_traffic_source PARTITION(ds='20250330')
select
    product_id,
    channel_id,
    product_name,
     stat_date,
     pv, uv,
     order_num,
     conversion_rate,
     order_user_num,
     payment_num,
     payment_amount,
     avg_price,
     refund_rate,
     score_avg,
    price_star,
     review_num,
     new_customer_review_num,
    old_customer_review_num,
    positive_review_num,
    negative_review_num,
    neutral_review_num,
    positive_ratio,
    negative_ratio
from (
                  select
                      opt.product_id,
                      opt.channel_id,
                      opb.product_name,
                      '20250330' as stat_date,
                      count(*) as pv,
                      count(distinct opt.user_id) as uv,
                      count(ops.order_id) as order_num,
                      round(count(distinct  ops.user_id)/count(ops.user_id)*100,2)as conversion_rate,
                      count(ops.user_id) as order_user_num,
                      sum(ops.quantity) as payment_num,
                      sum(payment_amount) as payment_amount,
                      sum(ops.payment_amount)/ count(ops.user_id) as avg_price,
                      count(oor.refund_type=1)/count(*) as refund_rate,
                      avg(opr.score) as score_avg,
      CASE
          WHEN sku.price <= price_bands.price_band_low THEN 5
         WHEN sku.price <= price_bands.price_band_mid THEN 4
         WHEN sku.price <= price_bands.price_band_high THEN 3
         ELSE 2
        END AS price_star,
                      count(opr.review_id) as review_num,
                      sum(`if`(opr.user_type='1',1,0)) as new_customer_review_num,
                      sum(`if`(opr.user_type='2',1,0)) as old_customer_review_num,
                      sum(`if`(opr.score>='4',1,0))as positive_review_num,
                      sum(`if`(opr.score<='2',1,0)) as negative_review_num,
                      sum(`if`(opr.score='3',1,0)) as neutral_review_num,
                      round(sum(`if`(opr.score>='4',1,0))*1.0/ count(opr.review_id),2) as positive_ratio,
                      round(sum(`if`(opr.score<='2',1,0))*1.0/count(opr.review_id),2) as negative_ratio
                  from ods_product_base opb
                           left join ods_product_traffic opt on opb.product_id = opt.product_id
                           left join ods_product_sales ops on opb.product_id = ops.product_id
                           left join ods_order_refund oor on opb.product_id = oor.product_id
                           left join ods_product_keyword opk on opk.product_id=opb.product_id
                           left join ods_product_review opr on opt.product_id = opr.product_id
                           left join ods_product_sku sku on oor.product_id = sku.product_id
  left join (
      SELECT
          opb.category_id,
         MIN(sku.price) AS price_band_low,
        MAX(sku.price) * 0.3 AS price_band_mid,
          MAX(sku.price) * 0.7 AS price_band_high
     FROM ods_product_base opb
               JOIN ods_product_sku sku ON opb.product_id = sku.product_id
    WHERE sku.ds = '20250330'
     GROUP BY opb.category_id
  ) price_bands ON opb.category_id = price_bands.category_id
                   group by
                      opt.product_id,
                      opt.channel_id,
                      opb.product_name,
      CASE
          WHEN sku.price <= price_bands.price_band_low THEN 5
          WHEN sku.price <= price_bands.price_band_mid THEN 4
          WHEN sku.price <= price_bands.price_band_high THEN 3
          ELSE 2
          END

)t1 group by product_id, channel_id, product_name, stat_date, pv, uv, order_num, conversion_rate,
             order_user_num, payment_num, payment_amount, avg_price, refund_rate, score_avg, price_star,review_num,
             new_customer_review_num, old_customer_review_num,
             positive_review_num, negative_review_num, neutral_review_num, positive_ratio, negative_ratio;
