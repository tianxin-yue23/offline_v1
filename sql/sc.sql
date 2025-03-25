set hive.exec.mode.local.auto=true;
use dev_realtime_v1_tianxin_yue;

DROP TABLE IF EXISTS `ods_order_info`;
CREATE TABLE `ods_order_info`  (
                                   `id` bigint COMMENT '编号',
                                   `consignee` varchar(100)    COMMENT '收货人',
                                   `consignee_tel` varchar(20)    COMMENT '收件人电话',
                                   `total_amount` decimal(10, 2)  COMMENT '总金额',
                                   `order_status` varchar(20)    COMMENT '订单状态',
                                   `user_id` bigint  COMMENT '用户id',
                                   `payment_way` varchar(20)    COMMENT '付款方式',
                                   `delivery_address` varchar(1000)    COMMENT '送货地址',
                                   `order_comment` varchar(200)    COMMENT '订单备注',
                                   `out_trade_no` varchar(50)    COMMENT '订单交易编号（第三方支付用)',
                                   `trade_body` varchar(200)    COMMENT '订单描述(第三方支付用)',
                                   `create_time` string COMMENT '创建时间',
                                   `operate_time` string  COMMENT '操作时间',
                                   `expire_time`string  COMMENT '失效时间',
                                   `process_status` varchar(20)    COMMENT '进度状态',
                                   `tracking_no` varchar(100)    COMMENT '物流单编号',
                                   `parent_order_id` bigint COMMENT '父订单编号',
                                   `img_url` varchar(200)    COMMENT '图片路径',
                                   `province_id` int  COMMENT '地区',
                                   `activity_reduce_amount` decimal(16, 2)  COMMENT '促销金额',
                                   `coupon_reduce_amount` decimal(16, 2)  COMMENT '优惠券',
                                   `original_total_amount` decimal(16, 2)  COMMENT '原价金额',
                                   `feight_fee` decimal(16, 2)  COMMENT '运费',
                                   `feight_fee_reduce` decimal(16, 2)  COMMENT '运费减免',
                                   `refundable_time` string  COMMENT '可退款日期（签收后30天）'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/order_info/2025-03-23"  into table ods_order_info partition (dt="2025-03-23");




DROP TABLE IF EXISTS `ods_order_detail`;
CREATE TABLE `ods_order_detail`  (
                                     `id` bigint COMMENT '编号',
                                     `order_id` bigint   COMMENT '订单编号',
                                     `sku_id` bigint   COMMENT 'sku_id',
                                     `sku_name` varchar(200)    COMMENT 'sku名称（冗余)',
                                     `img_url` varchar(200)    COMMENT '图片名称（冗余)',
                                     `order_price` decimal(10, 2)   COMMENT '购买价格(下单时sku价格）',
                                     `sku_num` varchar(200)    COMMENT '购买个数',
                                     `create_time` string   COMMENT '创建时间',
                                     `source_type` varchar(20)    COMMENT '来源类型',
                                     `source_id` bigint  COMMENT '来源编号',
                                     `split_total_amount` decimal(16, 2)  ,
                                     `split_activity_amount` decimal(16, 2)  ,
                                     `split_coupon_amount` decimal(16, 2)

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/order_detail/2025-03-23"into table ods_order_detail partition (dt="2025-03-23");

DROP TABLE IF EXISTS `spu_info`;
CREATE TABLE `ods_spu_info`  (
                                 `id` bigint COMMENT '商品id',
                                 `spu_name` varchar(200)   COMMENT '商品名称',
                                 `description` varchar(1000)   COMMENT '商品描述(后台简述）',
                                 `category3_id` bigint COMMENT '三级分类id',
                                 `tm_id` bigint COMMENT '品牌id'

) partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/spu_info/2025-03-23"into table ods_spu_info partition (dt="2025-03-23");

DROP TABLE IF EXISTS `sku_info`;
CREATE TABLE `ods_sku_info`  (
                                 `id` bigint COMMENT '库存id(itemID)',
                                 `spu_id` bigint  COMMENT '商品id',
                                 `price` decimal(10, 0)  COMMENT '价格',
                                 `sku_name` varchar(200)   COMMENT 'sku名称',
                                 `sku_desc` varchar(2000)  COMMENT '商品规格描述',
                                 `weight` decimal(10, 2)  COMMENT '重量',
                                 `tm_id` bigint COMMENT '品牌(冗余)',
                                 `category3_id` bigint  COMMENT '三级分类id（冗余)',
                                 `sku_default_img` varchar(300)  COMMENT '默认显示图片(冗余)',
                                 `is_sale` tinyint  COMMENT '是否销售（1：是 0：否）',
                                 `create_time` string  COMMENT '创建时间'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/sku_info/2025-03-23"into table ods_sku_info partition (dt="2025-03-23");


DROP TABLE IF EXISTS `base_province`;
CREATE TABLE `ods_base_province`  (
                                      `id` bigint COMMENT 'id',
                                      `name` varchar(20) COMMENT '省名称',
                                      `region_id` varchar(20) COMMENT '大区id',
                                      `area_code` varchar(20) COMMENT '行政区位码',
                                      `iso_code` varchar(20) COMMENT '国际编码',
                                      `iso_3166_2` varchar(20) COMMENT 'ISO3166编码'
)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/base_province/2025-03-23"into table ods_base_province partition (dt="2025-03-23");


DROP TABLE IF EXISTS `base_category1`;
CREATE TABLE `ods_base_category1`  (
                                       `id` bigint COMMENT '编号',
                                       `name` varchar(10) COMMENT '分类名称'

) partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/base_category1/2025-03-23"into table ods_base_category1 partition (dt="2025-03-23");


DROP TABLE IF EXISTS `base_category2`;
CREATE TABLE `ods_base_category2`  (
                                       `id` bigint COMMENT '编号',
                                       `name` varchar(200)  COMMENT '二级分类名称',
                                       `category1_id` bigint COMMENT '一级分类编号'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/base_category2/2025-03-23"into table ods_base_category2 partition (dt="2025-03-23");


DROP TABLE IF EXISTS `base_category3`;
CREATE TABLE `ods_base_category3`  (
                                       `id` bigint COMMENT '编号',
                                       `name` varchar(200)  COMMENT '三级分类名称',
                                       `category2_id` bigint COMMENT '二级分类编号'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/base_category3/2025-03-23"into table ods_base_category3 partition (dt="2025-03-23");

DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `ods_user_info`  (
                                  `id` bigint COMMENT '编号',
                                  `login_name` varchar(200)  COMMENT '用户名称',
                                  `nick_name` varchar(200)  COMMENT '用户昵称',
                                  `passwd` varchar(200)  COMMENT '用户密码',
                                  `name` varchar(200)  COMMENT '用户姓名',
                                  `phone_num` varchar(200)  COMMENT '手机号',
                                  `email` varchar(200)  COMMENT '邮箱',
                                  `head_img` varchar(200)  COMMENT '头像',
                                  `user_level` varchar(200)  COMMENT '用户级别',
                                  `birthday` string COMMENT '用户生日',
                                  `gender` varchar(1)  COMMENT '性别 M男,F女',
                                  `create_time` string COMMENT '创建时间',
                                  `operate_time` string COMMENT '修改时间',
                                  `status` varchar(200)  COMMENT '状态'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/user_info/2025-03-23"into table ods_user_info partition (dt="2025-03-23");

DROP TABLE IF EXISTS `base_dic`;
CREATE TABLE `ods_base_dic`  (
                                 `dic_code` varchar(10)  COMMENT '编号',
                                 `dic_name` varchar(100)  COMMENT '编码名称',
                                 `parent_code` varchar(10)  COMMENT '父编号',
                                 `create_time` string COMMENT '创建日期',
                                 `operate_time` string COMMENT '修改日期'
) partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/base_dic/2025-03-23"into table ods_base_dic partition (dt="2025-03-23");

DROP TABLE IF EXISTS `ods_payment_info`;
CREATE TABLE `ods_payment_info`  (
                                     `id` int COMMENT '编号',
                                     `out_trade_no` varchar(50)  COMMENT '对外业务编号',
                                     `order_id` bigint  COMMENT '订单编号',
                                     `user_id` bigint ,
                                     `payment_type` varchar(20)  COMMENT '支付类型（微信 支付宝）',
                                     `trade_no` varchar(50)  COMMENT '交易编号',
                                     `total_amount` decimal(10, 2)  COMMENT '支付金额',
                                     `subject` varchar(200)  COMMENT '交易内容',
                                     `payment_status` varchar(20)  COMMENT '支付状态',
                                     `create_time` string  COMMENT '创建时间',
                                     `callback_time` string  COMMENT '回调时间',
                                     `callback_content` string COMMENT '回调信息'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/payment_info/2025-03-23"into table ods_payment_info partition (dt="2025-03-23");


DROP TABLE IF EXISTS `ods_order_detail_activity`;
CREATE TABLE `ods_order_detail_activity`  (
                                              `id` bigint COMMENT '编号',
                                              `order_id` bigint  COMMENT '订单id',
                                              `order_detail_id` bigint  COMMENT '订单明细id',
                                              `activity_id` bigint  COMMENT '活动ID',
                                              `activity_rule_id` bigint  COMMENT '活动规则',
                                              `sku_id` bigint  COMMENT 'skuID',
                                              `create_time` string  COMMENT '获取时间'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/order_detail_activity/2025-03-23"into table ods_order_detail_activity partition (dt="2025-03-23");



DROP TABLE IF EXISTS `ods_order_detail_coupon`;
CREATE TABLE `ods_order_detail_coupon`  (
                                            `id` bigint COMMENT '编号',
                                            `order_id` bigint  COMMENT '订单id',
                                            `order_detail_id` bigint  COMMENT '订单明细id',
                                            `coupon_id` bigint  COMMENT '购物券ID',
                                            `coupon_use_id` bigint  COMMENT '购物券领用id',
                                            `sku_id` bigint  COMMENT 'skuID',
                                            `create_time` string  COMMENT '获取时间'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
load data inpath "/2207A/tianxin_yue/order_detail_coupon/2025-03-23"into table ods_order_detail_coupon partition (dt="2025-03-23");

-------------dwd---------------
-- 创建DWD层的交易域下单事务事实表
drop table if exists dwd_order_detail;
CREATE TABLE `dwd_order_detail`  (
                                     `id` bigint COMMENT '编号',
                                     `order_id` bigint   COMMENT '订单编号',
                                     `sku_id` bigint   COMMENT 'sku_id',
                                     `sku_name` varchar(200)    COMMENT 'sku名称（冗余)',
                                     `img_url` varchar(200)    COMMENT '图片名称（冗余)',
                                     `order_price` decimal(10, 2)   COMMENT '购买价格(下单时sku价格）',
                                     `sku_num` varchar(200)    COMMENT '购买个数',
                                     `create_time` string   COMMENT '创建时间',
                                     `source_type` varchar(20)    COMMENT '来源类型',
                                     `source_id` bigint  COMMENT '来源编号',
                                     `split_total_amount` decimal(16, 2)  ,
                                     `split_activity_amount` decimal(16, 2)  ,
                                     `split_coupon_amount` decimal(16, 2),
                                     tm_id bigint comment '品牌',
                                     user_id bigint comment '用户',
                                     source_type_name string,
                                     province_id string comment '地区',
                                     province_name string

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
insert overwrite table `dwd_order_detail`
select
    od.id,
    od.order_id,
    od.sku_id,
    od.sku_name,
    od.img_url,
    od.order_price,
    od.sku_num,
    od.create_time,
    od.source_type,
    od.source_id,
    od.split_total_amount,
    od.split_activity_amount,
    od.split_coupon_amount,
    si.tm_id,
    oi.user_id,
    bd.dic_name,
    oi.province_id,
    obp.name,
    od.dt
from ods_order_detail od
         left join ods_sku_info si on si.id=od.sku_id
         left join ods_order_info oi on oi.id=od.order_id
         left join ods_base_dic bd on od.source_id=bd.dic_code
         left  join ods_base_province obp on oi.province_id = obp.id;
-- 创建DWD层的工具域-优惠券领取-事务事实表
CREATE TABLE `dwd_order_detail_coupon`  (
                                            `id` bigint COMMENT '编号',
                                            `order_id` bigint  COMMENT '订单id',
                                            `order_detail_id` bigint  COMMENT '订单明细id',
                                            `coupon_id` bigint  COMMENT '购物券ID',
                                            `coupon_use_id` bigint  COMMENT '购物券领用id',
                                            `sku_id` bigint  COMMENT 'skuID',
                                            `create_time` string  COMMENT '获取时间'

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
insert into dwd_order_detail_coupon
select
    id,
    order_id,
    order_detail_id,
    coupon_id,
    coupon_use_id,
    sku_id,
    create_time, dt
from ods_order_detail_coupon;

-- 创建DWD层的交易域-支付成功-事务事实表

CREATE TABLE `dwd_payment_info`  (
                                     `id` int COMMENT '编号',
                                     `out_trade_no` varchar(50)  COMMENT '对外业务编号',
                                     `order_id` bigint  COMMENT '订单编号',
                                     `user_id` bigint ,
                                     `payment_type` varchar(20)  COMMENT '支付类型（微信 支付宝）',
                                     `trade_no` varchar(50)  COMMENT '交易编号',
                                     `total_amount` decimal(10, 2)  COMMENT '支付金额',
                                     `subject` varchar(200)  COMMENT '交易内容',
                                     `payment_status` varchar(20)  COMMENT '支付状态',
                                     `create_time` string  COMMENT '创建时间',
                                     `callback_time` string  COMMENT '回调时间',
                                     `callback_content` string COMMENT '回调信息',
                                     coupon_id  bigint

)partitioned by (dt string)
    row format delimited fields terminated by "\t";
insert into dwd_payment_info
select
    opi.id,
    opi.out_trade_no,
    opi.order_id,
    opi.user_id,
    opi.payment_type,
    opi.trade_no,
    opi.total_amount,
    opi.subject,
    opi.payment_status,
    opi.create_time,
    opi.callback_time,
    opi.callback_content,
    oodc.coupon_id,
    opi.dt
from ods_payment_info opi
         left join ods_order_detail_coupon oodc on opi.order_id = oodc.order_id;
---------------------dws----------------------
-- 最近1日的各省份交易统计
create table if not exists dws_order_province_1d(
                                                    dt string,
                                                    pname string,
                                                    order_num bigint,
                                                    order_amount decimal(19,2)
    )row format delimited fields terminated by "\t";
insert into dws_order_province_1d
select '2025-03-23',
       province_name,
       count(distinct order_id),
       sum(split_total_amount)
from dwd_order_detail where dt='2025-03-23'
group by province_name;
-- 最近30日的交易综合统计
create table if not exists dws_num_30d(
                                          dt string,
                                          order_num bigint,
                                          order_amount decimal(19,2)
    )row format delimited fields terminated by "\t";
insert into dws_num_30d
select '2025-03-23',
       sum(order_num),
       sum(order_amount)
from dws_order_province_1d
where dt>=date_sub('2025-03-23',29);
--------------------ads-------------------
--复购率
create table if not exists ads_tm_fgl(
                                         dt string,
                                         tm_id string,
                                         fgl decimal(19,2)
    )row format delimited fields terminated by "\t";

--按月汇总各个品牌的购买人数


--按月汇总-各个品牌的复购人数
--一个人一个月购买同一个品牌 超过两次及以上
insert into ads_tm_fgl
select
    t1.dt_month,
    t1.tm_id,
    nvl(c1/c2 * 100, 0) as ratio
from (
         -- 按月汇总各个品牌的购买人数
         select substr(dt, 1, 7) dt_month,tm_id, count(distinct user_id) c2
         from dwd_order_detail
         group by substr(dt, 1, 7), tm_id
     ) t1 left join (
    -- 按月汇总各个品牌的复购人数
    select dt_month,tm_id,count(*) c1
    from (
             select substr(dt, 1, 7) dt_month,tm_id,user_id,order_id,
                    -- 每个用户在每个月每个品牌下的订单数量
                    count(distinct order_id) over (partition by substr(dt, 1, 7), tm_id, user_id) as order_count
             from dwd_order_detail
         ) sub
    where order_count >= 2 group by dt_month, tm_id
) t2 on t1.dt_month = t2.dt_month and t1.tm_id = t2.tm_id;
-- 按月统计订单来源数量和占比
create table if not exists ads_source_zb(
                                            dt string,
                                            source_id string,
                                            source_name string,
                                            source_num string,
                                            zb decimal(19,2)
    )row format delimited fields terminated by "\t";

insert into ads_source_zb
select t1.dt_month, t1.source_type, t1.source_type_name, c1,
       nvl(c1/c2*100,0)
from (
         --按月统计订单来源数量
         select substr(dt,1,7)dt_month,
                source_type,
                source_type_name,
                count(distinct order_id)c1
         from dwd_order_detail
         group by substr(dt,1,7), source_type, source_type_name
     )t1 left join (
    select substr(dt,1,7)dt_month,
           count(distinct order_id)c2
    from dwd_order_detail
    group by substr(dt,1,7)

)t2 on t1.dt_month=t2.dt_month;