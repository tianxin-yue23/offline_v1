set hive.exec.mode.local.auto=true;
use dev_realtime_v1_tianxin_yue;
--商品维度表
CREATE TABLE dim_product (
                             product_id STRING COMMENT '商品ID',
                             product_name STRING COMMENT '商品名称',
                             category_id STRING COMMENT '类目ID',
                             category_name STRING COMMENT '类目名称',
                             leaf_category_id STRING COMMENT '叶子类目ID',
                             leaf_category_name STRING COMMENT '叶子类目名称',
                             price DECIMAL(18,2) COMMENT '商品价格',
                             create_time string COMMENT '创建时间',
                             modify_time string COMMENT '修改时间',
                             is_active string COMMENT '是否有效(1:是,0:否)'
)partitioned by  (dt STRING);

insert into dim_product values
              ("P001","夏季男士短袖T恤","CAT01","男装","CAT0101","男士T恤",99.00,'2024-05-10 10:00:00','2024-05-10 10:00:00',"1",'2025-03-28'),
              ("P002","女士休闲牛仔裤","CAT02","女装","CAT0203","女士牛仔裤",159.00,"2024-05-15 14:30:00","2024-05-20 09:15:00","1",'2025-03-28'),
             ('P003','无线蓝牙耳机','CAT03','数码','CAT0305','耳机',299.00,'2024-06-01 08:00:00','2024-06-01 08:00:00',"1",'2025-03-28');

CREATE TABLE dim_date (
                          date_id STRING COMMENT '日期ID(YYYYMMDD)',
                          date_val DATE COMMENT '日期值',
                          day_of_week INT COMMENT '星期几(1-7)',
                          day_of_month INT COMMENT '当月第几天',
                          day_of_year INT COMMENT '当年第几天',
                          week_of_year INT COMMENT '当年第几周',
                          month INT COMMENT '月份',
                          month_name STRING COMMENT '月份名称',
                          quarter INT COMMENT '季度',
                          year INT COMMENT '年份',
                          is_weekend TINYINT COMMENT '是否周末(1:是,0:否)',
                          is_holiday TINYINT COMMENT '是否节假日(1:是,0:否)',
                          etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '时间维度表';



CREATE EXTERNAL TABLE ods_cart_info(
                                       `id` STRING COMMENT '编号',
                                       `user_id` STRING COMMENT '用户id',
                                       `sku_id` STRING COMMENT 'skuid',
                                       `cart_price` DECIMAL(16,2)  COMMENT '放入购物车时价格',
                                       `sku_num` BIGINT COMMENT '数量',
                                       `sku_name` STRING COMMENT 'sku名称 (冗余)',
                                       `create_time` STRING COMMENT '创建时间',
                                       `operate_time` STRING COMMENT '修改时间',
                                       `is_ordered` STRING COMMENT '是否已经下单',
                                       `order_time` STRING COMMENT '下单时间',
                                       `source_type` STRING COMMENT '来源类型',
                                       `source_id` STRING COMMENT '来源编号'
) COMMENT '加购表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
