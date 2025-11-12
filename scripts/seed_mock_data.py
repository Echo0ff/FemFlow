"""
生成并插入女性生理期相关的mock数据到MongoDB
"""
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from typing import List, Dict

# MongoDB连接配置
MONGO_URI = "mongodb://root:rootpassword@localhost:27017/"
DATABASE_NAME = "femflow"
COLLECTION_NAME = "menstrual_records"


def generate_mock_data(count: int = 50) -> List[Dict]:
    """
    生成mock生理期数据
    
    Args:
        count: 生成记录数量
        
    Returns:
        包含生理期记录的列表
    """
    records = []
    base_date = datetime.now() - timedelta(days=365)  # 从一年前开始
    
    for i in range(count):
        # 月经周期通常在21-35天之间
        cycle_length = random.randint(21, 35)
        
        # 月经持续时间通常在3-7天
        duration = random.randint(3, 7)
        
        # 计算月经开始日期（基于周期）
        period_start = base_date + timedelta(days=i * cycle_length + random.randint(-3, 3))
        period_end = period_start + timedelta(days=duration - 1)
        
        # 上次月经时间（前一个周期）
        if i > 0:
            last_cycle_length = random.randint(21, 35)
            last_duration = random.randint(3, 7)
            last_period_start = period_start - timedelta(days=last_cycle_length)
            last_period_end = last_period_start + timedelta(days=last_duration - 1)
        else:
            last_cycle_length = random.randint(21, 35)
            last_duration = random.randint(3, 7)
            last_period_start = period_start - timedelta(days=last_cycle_length)
            last_period_end = last_period_start + timedelta(days=last_duration - 1)
        
        # 白带异常（10%的概率）
        has_abnormal_discharge = random.random() < 0.1
        
        # 疼痛程度（0-5，0为无痛，5为剧痛）
        pain_level = random.choices(
            [0, 1, 2, 3, 4, 5],
            weights=[30, 25, 20, 15, 7, 3]  # 大多数情况下疼痛较轻
        )[0]
        
        # 情绪状态
        mood = random.choice(["正常", "轻微焦虑", "情绪低落", "易怒", "情绪稳定"])
        
        # 流量（轻、中、重）
        flow = random.choice(["轻", "中", "重"])
        
        record = {
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "duration_days": duration,
            "cycle_length_days": cycle_length,
            "has_abnormal_discharge": has_abnormal_discharge,
            "abnormal_discharge_description": "白带异常，颜色偏黄，有异味" if has_abnormal_discharge else None,
            "last_period_start": last_period_start.isoformat(),
            "last_period_end": last_period_end.isoformat(),
            "last_duration_days": last_duration,
            "last_cycle_length_days": last_cycle_length,
            "pain_level": pain_level,
            "mood": mood,
            "flow": flow,
            "notes": random.choice([
                None,
                "经期前有轻微腹痛",
                "经期第一天流量较大",
                "经期后几天流量减少",
                "本次周期较规律"
            ]),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        records.append(record)
    
    return records


def insert_data_to_mongodb(records: List[Dict]):
    """
    将数据插入MongoDB
    
    Args:
        records: 要插入的记录列表
    """
    try:
        # 连接MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # 清空现有数据（可选）
        collection.delete_many({})
        print(f"已清空集合 {COLLECTION_NAME} 中的现有数据")
        
        # 插入数据
        result = collection.insert_many(records)
        print(f"成功插入 {len(result.inserted_ids)} 条记录到 {DATABASE_NAME}.{COLLECTION_NAME}")
        
        # 显示统计信息
        total_count = collection.count_documents({})
        print(f"集合中总共有 {total_count} 条记录")
        
        # 显示一些示例数据
        sample = collection.find_one()
        if sample:
            print("\n示例记录:")
            for key, value in sample.items():
                if key != "_id":
                    print(f"  {key}: {value}")
        
        client.close()
        
    except Exception as e:
        print(f"插入数据时出错: {e}")
        raise


def main():
    """主函数"""
    print("开始生成mock数据...")
    records = generate_mock_data(count=50)
    print(f"生成了 {len(records)} 条mock数据")
    
    print("\n正在插入数据到MongoDB...")
    insert_data_to_mongodb(records)
    print("\n完成！")


if __name__ == "__main__":
    main()

