set hive.exec.mode.local.auto=true;
use dev_realtime_v1_tianxin_yue;


CREATE external TABLE dev_realtime_v1_tianxin_yue.ods_product_base_info (
                                       product_id STRING COMMENT '商品唯一标识',
                                       product_name STRING COMMENT '商品名称',
                                       category_id STRING COMMENT '商品分类ID',
                                       category_name STRING COMMENT '商品分类名称',
                                       brand_id STRING COMMENT '品牌ID',
                                       brand_name STRING COMMENT '品牌名称',
                                       price DECIMAL(18,2) COMMENT '销售价格',
                                       cost DECIMAL(18,2) COMMENT '成本价格',
                                       status TINYINT COMMENT '状态(1-上架,0-下架)',
                                       create_time TIMESTAMP COMMENT '创建时间',
                                       update_time TIMESTAMP COMMENT '更新时间'

) COMMENT '商品信息表'
    PARTITIONED BY (ds STRING)
    STORED AS orc
    tblproperties ("orc.compress"="snappy");
CREATE TABLE dev_realtime_v1_tianxin_yue.ods_store_base_info (
                                     store_id STRING COMMENT '店铺ID',
                                     store_name STRING COMMENT '店铺名称',
                                     region_id STRING COMMENT '区域ID',
                                     region_name STRING COMMENT '区域名称',
                                     city_id STRING COMMENT '城市ID',
                                     city_name STRING COMMENT '城市名称',
                                     address STRING COMMENT '详细地址',
                                     status string COMMENT '状态(1-营业,0-歇业)',
                                     create_time string COMMENT '创建时间',
                                     update_time string COMMENT '更新时间'
) COMMENT '店铺表'
    PARTITIONED BY (ds STRING)
    STORED AS orc
    tblproperties ("orc.compress"="snappy");


CREATE TABLE dev_realtime_v1_tianxin_yue.ods_product_transaction (
                                         order_id STRING COMMENT '订单ID',
                                         product_id STRING COMMENT '商品ID',
                                         store_id STRING COMMENT '店铺ID',
                                         user_id STRING COMMENT '用户ID',
                                         quantity INT COMMENT '购买数量',
                                         amount DECIMAL(18,2) COMMENT '实际支付金额',
                                         order_status TINYINT COMMENT '订单状态(1待支付 2已支付 3已取消 4已退款)',
                                         payment_type STRING COMMENT '支付方式',
                                         order_time TIMESTAMP COMMENT '下单时间',
                                         pay_time TIMESTAMP COMMENT '支付时间',
                                         is_new_user TINYINT COMMENT '是否新用户(1是 0否)'

) COMMENT '商品交易事实表'
    PARTITIONED BY (ds STRING)
    STORED AS orc
    tblproperties ("orc.compress"="snappy");

CREATE TABLE dev_realtime_v1_tianxin_yue.ods_product_behavior_log (
                                          log_id STRING COMMENT '日志ID',
                                          user_id STRING COMMENT '用户ID',
                                          product_id STRING COMMENT '商品ID',
                                          store_id STRING COMMENT '店铺ID',
                                          event_type STRING COMMENT '事件类型(view-浏览,cart-加购,fav-收藏)',
                                          duration_sec INT COMMENT '停留时长(秒)',
                                          device_type STRING COMMENT '设备类型',
                                          ip_address STRING COMMENT 'IP地址',
                                          event_time string COMMENT '事件时间'

) COMMENT '商品浏览行为日志表'
    PARTITIONED BY (ds STRING)
    STORED AS orc
    tblproperties ("orc.compress"="snappy");

CREATE EXTERNAL TABLE dev_realtime_v1_tianxin_yue.ods_order_refund (
                                           refund_id STRING COMMENT '退款ID',
                                           order_id STRING COMMENT '订单ID',
                                           product_id STRING COMMENT '商品ID',
                                           store_id STRING COMMENT '店铺ID',
                                           user_id STRING COMMENT '用户ID',
                                           refund_amount DECIMAL(18,2) COMMENT '退款金额',
                                           refund_type TINYINT COMMENT '退款类型(1仅退款 2退货退款)',
                                           refund_time TIMESTAMP COMMENT '退款时间',
                                           refund_reason STRING COMMENT '退款原因'
)
    PARTITIONED BY (ds STRING COMMENT '日期分区')
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="snappy");

CREATE TABLE dev_realtime_v1_tianxin_yue.dws_product_behavior_log (

                                    user_id STRING COMMENT '用户ID',
                                    product_id STRING COMMENT '商品ID',
                                    store_id STRING COMMENT '店铺ID',
                                    product_name STRING COMMENT '商品名称',
                                    product_num_uv string comment '商品访客数',
                                    product_num_pv string comment '商品浏览量',
                                    have_product_num string comment '有访问商品数',
                                    product_stopover_avg_ts string comment '商品平均停留时长',
                                    product_info_page_jump_rate string comment '商品详情页跳出率',
                                    product_cart_uid string comment'商品加购人数',
                                    product_fav_num string comment '商品收藏人数',
                                    product_cart_num string comment '商品加购件数',
                                    product_fav_zb string comment '访问收藏转化率',
                                    product_cart_zb string comment '访问加购转化率',
                                    product_mai_num string comment '下单买家数',
                                    product_jian_num string comment '下单件数',
                                    product_zhuan_lv string comment '下单转化率',
                                    payment_mai string comment '支付买家数',
                                    payment_num string comment '支付件数',
                                    payment_amount DECIMAL(18,2) comment '支付金额',
                                    payment_spu string comment '有支付商品数',
                                    payment_zh string comment ' 支付转化率',
                                    payment_new_mai string comment '支付新买家数',
                                    payment_old_mai string comment '支付老买家数',
                                    payment_old_amount string comment '老买家支付金额',
                                    refund_amount DECIMAL(18,2) comment '成功退款退货金额',
                                    year_payment_amount DECIMAL(18,2) comment '年累计支付金额'
)
    PARTITIONED BY (ds STRING)
    STORED AS orc
    tblproperties ("orc.compress"="snappy");

insert into dev_realtime_v1_tianxin_yue.dws_product_behavior_log   PARTITION(ds='20250330')
SELECT
    user_id,
    product_id,
    store_id,
    product_name,
    uv,
    pv,--商品浏览量
    product_view_num,
    pj,--商品平均停留时长
    ROUND(SUM(IF(page_view = 1, 1, 0)) * 100.0 / COUNT(*), 2) AS product_jump_rate,--跳出率,
    product_cart_user,
    product_fav_num,
    product_cart_num,
    product_fav_zb,
    product_cart_zb,
    product_mai_num,
    product_jian_num,
    product_zhuan_lv,
    payment_mai,
    payment_num,
    payment_amount,
    payment_spu,
    payment_zh,
    payment_new_mai,
    payment_old_mai,
    payment_old_amount,
    refund_amount,
    year_payment_amount
FROM (
         SELECT
             opbl.product_id,
             opbi.product_name,
             opbl.store_id,
             opbl.user_id,
             count(*) as pv,
             COUNT(`if`(opbl.event_type='view',1,0)) AS page_view,
             count(distinct opbl.user_id)as uv,--商品访客数
             count(distinct opbl.product_id) as product_view_num,--有访问商品数
             avg(opbl.duration_sec) as pj,--商品平均停留时长
--              -- 使用用户ID和会话开始时间戳生成会话ID
              CONCAT(opbl.user_id, '-',
                     DATE_FORMAT(MIN(opbl.event_time), '%Y%m%d%H')) AS session_id,
             count(if(opbl.event_type='cart',opbl.user_id,0)) as product_cart_user,--商品加购人数
             count(`if`(opbl.event_type='fav',1,0)) as product_fav_num,--商品收藏人数
             sum(`if`(opbl.event_type='cart',1,0)) as product_cart_num,--商品加购件数
             round(sum(`if`(opbl.event_type='fav',1,0))*1.0/count(*),2) as product_fav_zb,--访问收藏转化率,
             round(sum(`if`(opbl.event_type='cart',1,0))*1.0/count(*),2) as product_cart_zb,--访问加购转化率,
             COUNT(DISTINCT opbl.user_id) as product_mai_num,--下单买家数
             SUM(opt.quantity) as product_jian_num,--下单件数
             ROUND( COUNT(DISTINCT opbl.user_id,1,0)* 100.0 / (count(opbl.user_id)), 2) AS product_zhuan_lv, -- 下单转化率
            count(`if`(opt.order_status='2',opt.user_id,0))as payment_mai, --支付买家数'7
             count(`if`(opt.order_status='2',quantity,0))as payment_num, --支付件数,
             sum(amount)as payment_amount ,--支付金额,
             count(`if`(order_status='2',opt.product_id,0))as payment_spu,--有支付商品数,
             ROUND(COUNT(`if`(opt.order_status = '2',opt.user_id,0)) * 100.0/(count(distinct opbl.user_id)),2) AS payment_zh, -- 支付转化率
             count(`if`(opt.order_status='2' and opt.is_new_user=1,opt.user_id,0))as payment_new_mai,--支付新买家数,
             count(`if`(opt.order_status='2' and opt.is_new_user=0,opt.user_id,0))as payment_old_mai,--支付老买家数,
             sum(`if`(opt.order_status='2' and opt.is_new_user=0,opt.amount,0))as payment_old_amount,--老买家支付金额,
              oor.refund_amount as refund_amount, --成功退款退货金额,
            sum(`if`(year(opt.pay_time)= YEAR(CURRENT_DATE) and opt.order_status=2,amount,0))as year_payment_amount --年累计支付金额

         FROM ods_product_behavior_log opbl
         left join ods_product_base_info opbi on opbl.product_id = opbi.product_id
         left join ods_product_transaction opt on opt.user_id=opbl.user_id
         left join ods_order_refund oor on oor.user_id=opbl.user_id
--          where event_type = 'view'
         GROUP BY opbl.product_id, opbi.product_name, opbl.store_id,opbl.user_id, DATE_FORMAT(opbl.event_time, '%Y%m%d%H')
         ,oor.refund_amount
     ) t1
GROUP BY product_id, user_id, store_id, product_name, uv, pv, product_view_num, pj, product_cart_user, product_fav_num,
         product_cart_num, product_fav_zb, product_cart_zb, product_mai_num, product_jian_num, product_zhuan_lv,
         payment_mai, payment_num, payment_amount, payment_spu, payment_zh, payment_new_mai,
         payment_old_mai, payment_old_amount, refund_amount, year_payment_amount;



