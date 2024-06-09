import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import time
def window_stat(df_all, now_dt, window_size, name):
    
    dt_start = now_dt - window_size
    dt_str = datetime.datetime.strftime(now_dt, "%Y%m%d-%H")
    df = df_all[df_all.timestamp > dt_start]
    # 各类别交易金额的分布
    plt.figure(figsize=(14, 6))
    sns.boxplot(data=df, x='category', y='amt')
    plt.title('Transaction Amount Distribution by Category')
    plt.xlabel('Category')
    plt.ylabel('Transaction Amount')
    plt.xticks(rotation=45)
    # plt.show()
    plt.savefig(f"stat/amt_stat_{name}_{dt_str}.jpg")
    plt.close()

    # 不同性别在不同交易类别中的分布
    plt.figure(figsize=(14, 6))
    sns.countplot(data=df, x='category', hue='gender')
    plt.title('Gender Distribution across Transaction Categories')
    plt.xlabel('Category')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    # plt.show()
    plt.savefig(f"stat/gender_stat_{name}_{dt_str}.jpg")
    plt.close()

    # # 不同职业在不同交易类别中的分布
    # plt.figure(figsize=(14, 6))
    # sns.countplot(data=df, x='category', hue='job')
    # plt.title('Job Distribution across Transaction Categories')
    # plt.xlabel('Category')
    # plt.ylabel('Count')
    # plt.xticks(rotation=45)
    # # plt.show()
    # plt.savefig(f"stat/job_stat_{name}_{dt_str}.jpg")
    # plt.close()


    # # 城市人口与交易金额的关系
    # plt.figure(figsize=(10, 6))
    # sns.scatterplot(data=df, x='city_pop', y='amt', hue='category')
    # plt.title('City Population vs. Transaction Amount by Category')
    # plt.xlabel('City Population')
    # plt.ylabel('Transaction Amount')
    # # plt.show()
    # plt.savefig(f"stat/city_pop_amt_stat_{name}_{dt_str}.jpg")
    # plt.close()

    # # 不同职业的平均交易金额对比图
    # plt.figure(figsize=(14, 6))
    # sns.barplot(data=df, x='job', y='amt')
    # plt.title('Average Transaction Amount by Job')
    # plt.xlabel('Job')
    # plt.ylabel('Average Transaction Amount')
    # plt.xticks(rotation=45)
    # # plt.show()
    # plt.savefig(f"stat/job_amt_stat_{name}_{dt_str}.jpg")
    # plt.close()

    
    # 分析信用评分分布
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='score', kde=True)
    plt.title('Credit Score Distribution')
    plt.xlabel('Credit Score')
    plt.ylabel('Frequency')
    # plt.show()
    plt.savefig(f"stat/score_stat_freq_{name}_{dt_str}.jpg")
    plt.close()


    # 信用评分与交易金额的关系
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='score', y='amt', hue='category')
    plt.title('Credit Score vs. Transaction Amount by Category')
    plt.xlabel('Credit Score')
    plt.ylabel('Transaction Amount')
    # plt.show()
    plt.savefig(f"stat/score_amt_{name}_{dt_str}.jpg")
    plt.close()


    # 信用评分与城市人口的关系
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='score', y='city_pop', hue='category')
    plt.title('Credit Score vs. City Population by Category')
    plt.xlabel('Credit Score')
    plt.ylabel('City Population')
    # plt.show()
    plt.savefig(f"stat/score_city_pop_{name}_{dt_str}.jpg")
    plt.close()





def update_stat_results(df):
    """
    接受一个DataFrame，并生成多种可视化图表。
    
    参数:
    df (DataFrame): 包含交易数据的DataFrame
    
    """

    # now_dt = datetime.datetime.now(tz=datetime.timezone.utc)
    
    # 转换 Unix 时间为可读格式
    df['timestamp'] = pd.to_datetime(df['unix_time'], unit='s')

    now_dt = df['timestamp'].max()
    print(now_dt)

    t1 = time.time()
    for window_size, name in [
        (datetime.timedelta(hours=1), "1h"),
        (datetime.timedelta(hours=8), "8h"),
        (datetime.timedelta(days=1), "1d"),
    ]:
        window_stat(df, now_dt, window_size,  name)
    t2 = time.time()
    print("update plot time", t2-t1)

