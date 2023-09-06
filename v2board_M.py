import pymysql
import configparser
import ast
import time

# 读取配置文件
config = configparser.ConfigParser()
config.read('config.ini')

# 获取数据库连接配置
db_config = {
    'host': config.get('database', 'host'),
    'user': config.get('database', 'user'),
    'password': config.get('database', 'password'),
    'database': config.get('database', 'database')
}

# 获取更新参数
update_params = {
    'seconds': ast.literal_eval(config.get('update_params', 'seconds')),  # 通过配置文件获取 seconds
    'transfer': config.getint('update_params', 'transfer') / 1073741824  # 将 transfer 转换为 GB
}

# 获取订单号列表
trade_numbers = [number.strip() for number in config.get('trade_numbers', 'numbers').split(',')]

# 构建查询条件，查询支付完成订单号选取尾数两位
trade_numbers_condition = ', '.join(["'" + number + "'" for number in trade_numbers])
select_query = f"SELECT user_id FROM v2_order WHERE status = 3 AND SUBSTRING(trade_no, -2) IN ({trade_numbers_condition})"

update_query = "UPDATE v2_user SET expired_at = CASE " \
               "WHEN expired_at IS NULL THEN NULL " \
               "ELSE expired_at + %(seconds)s END, t = t + %(transfer)s WHERE user_id = %s"

# 连接数据库
with pymysql.connect(**db_config, cursorclass=pymysql.cursors.DictCursor) as connection:
    # 循环监测
    while True:
        try:
            with connection.cursor() as cursor:
                # 执行查询语句获取订单信息
                cursor.execute(select_query)
                orders = cursor.fetchall()

                # 循环处理每个订单
                for order in orders:
                    user_id = order['user_id']

                    # 执行更新语句
                    cursor.execute(update_query, update_params, user_id)

                # 提交更改
                connection.commit()

                # 打印成功信息
                if cursor.rowcount > 0:
                    print("成功更新用户信息和流量。")

                # 休眠一段时间后继续监测
                time.sleep(10)

        except KeyboardInterrupt:
            # 捕获键盘中断信号，退出循环
            break
        except Exception as e:
            # 捕获其他异常并打印错误信息
            print("发生错误:", str(e))
            # 休眠一段时间后继续监测
            time.sleep(10)
