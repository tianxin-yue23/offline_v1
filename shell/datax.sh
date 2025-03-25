
DATAX_HOME=/opt/soft/datax

# 如果传入日期则do_date等于传入的日期，否则等于前一天日�
#处理目标路径，此处的处理逻辑是，如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
handle_targetdir() {
  hadoop fs -test -e $1
  if [[ $? -eq 1 ]]; then
    echo "路径$1不存在，正在创建......"
    hadoop fs -mkdir -p $1
  else
    echo "路径$1已经存在"
    fs_count=$(hadoop fs -count $1)
    content_size=$(echo $fs_count | awk '{print $3}')
    if [[ $content_size -eq 0 ]]; then
      echo "路径$1为空"
    else
      echo "路径$1不为空，正在清空......"
      hadoop fs -rm -r -f $1/*
    fi
  fi
}
#数据同步
import_data() {
  datax_config=$1
  target_dir=$2

  handle_targetdir $target_dir
  python3 $DATAX_HOME/bin/datax.py -p"-Dtargetdir=$target_dir" $datax_config

}
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.base_category1.json /2207A/tianxin_yue/base_category1/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.base_category2.json /2207A/tianxin_yue/base_category2/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.base_category3.json /2207A/tianxin_yue/base_category3/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.order_info.json /2207A/tianxin_yue/order_info/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.base_dic.json /2207A/tianxin_yue/base_dic/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.base_province.json /2207A/tianxin_yue/base_province/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.order_detail.json /2207A/tianxin_yue/order_detail/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.sku_info.json /2207A/tianxin_yue/sku_info/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.spu_info.json  /2207A/tianxin_yue/spu_info/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.user_info.json /2207A/tianxin_yue/user_info/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.payment_info.json /2207A/tianxin_yue/payment_info/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.order_detail_activity.json /2207A/tianxin_yue/order_detail_activity/2025-03-23
import_data /opt/soft/datax/job/yuetianxin/import/dev_realtime_v1_tianxin_yue.order_detail_coupon.json /2207A/tianxin_yue/order_detail_coupon/2025-03-23
