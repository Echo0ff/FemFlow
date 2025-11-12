"""
MongoDB 常用查询方式示例
演示基于之前创建的生理期数据的各种查询操作
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
from typing import Dict, List

# MongoDB连接配置
MONGO_URI = "mongodb://root:rootpassword@localhost:27017/"
DATABASE_NAME = "femflow"
COLLECTION_NAME = "menstrual_records"


def connect_to_mongo():
    """连接到MongoDB"""
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    return client, collection


def example_1_basic_find(collection):
    """示例1: 基础查询 - find() 和 findOne()"""
    print("=" * 60)
    print("示例1: 基础查询")
    print("=" * 60)

    # 1. 查找所有文档
    all_records = list(collection.find())
    print(f"总记录数: {len(all_records)}")

    # 2. 查找单个文档
    one_record = collection.find_one()
    print(f"\n单条记录示例:")
    print(f"  开始时间: {one_record.get('period_start')}")
    print(f"  持续时间: {one_record.get('duration_days')} 天")

    # 3. 限制返回数量
    recent_5 = list(collection.find().limit(5))
    print(f"\n最近5条记录: {len(recent_5)} 条")


def example_2_filter_queries(collection):
    """示例2: 条件筛选查询"""
    print("\n" + "=" * 60)
    print("示例2: 条件筛选查询")
    print("=" * 60)

    # 1. 等值查询
    abnormal_records = list(collection.find({"has_abnormal_discharge": True}))
    print(f"有白带异常的记录: {len(abnormal_records)} 条")

    # 2. 范围查询 ($gt, $gte, $lt, $lte)
    long_periods = list(collection.find({"duration_days": {"$gte": 6}}))
    print(f"持续时间 >= 6天的记录: {len(long_periods)} 条")

    # 3. 多条件查询 ($and, $or)
    complex_query = {
        "$and": [{"duration_days": {"$gte": 5}}, {"pain_level": {"$gte": 3}}]
    }
    painful_long = list(collection.find(complex_query))
    print(f"持续时间>=5天且疼痛>=3的记录: {len(painful_long)} 条")

    # 4. 包含查询 ($in)
    moods = list(collection.find({"mood": {"$in": ["易怒", "情绪低落"]}}))
    print(f"情绪为易怒或低落的记录: {len(moods)} 条")

    # 5. 存在性查询 ($exists)
    with_notes = list(collection.find({"notes": {"$exists": True, "$ne": None}}))
    print(f"有备注的记录: {len(with_notes)} 条")


def example_3_sort_and_limit(collection):
    """示例3: 排序和分页"""
    print("\n" + "=" * 60)
    print("示例3: 排序和分页")
    print("=" * 60)

    # 1. 排序 (1=升序, -1=降序)
    recent_records = list(
        collection.find()
        .sort("period_start", -1)  # 按开始时间降序（最新的在前）
        .limit(5)
    )
    print("最近5条记录（按时间降序）:")
    for record in recent_records:
        print(f"  {record.get('period_start')} - {record.get('duration_days')}天")

    # 2. 多字段排序
    sorted_by_duration = list(
        collection.find()
        .sort([("duration_days", -1), ("pain_level", -1)])  # 先按持续时间，再按疼痛程度
        .limit(3)
    )
    print("\n持续时间最长且疼痛最严重的3条记录:")
    for record in sorted_by_duration:
        print(
            f"  持续时间: {record.get('duration_days')}天, "
            f"疼痛: {record.get('pain_level')}, "
            f"开始: {record.get('period_start')}"
        )


def example_4_projection(collection):
    """示例4: 字段投影（只返回需要的字段）"""
    print("\n" + "=" * 60)
    print("示例4: 字段投影")
    print("=" * 60)

    # 只返回指定字段 (1=包含, 0=排除)
    # _id 默认返回，如果不需要可以设为0
    minimal_data = list(
        collection.find(
            {},
            {
                "period_start": 1,
                "duration_days": 1,
                "has_abnormal_discharge": 1,
                "_id": 0,  # 排除_id字段
            },
        ).limit(3)
    )
    print("只返回关键字段的记录:")
    for record in minimal_data:
        print(f"  {record}")


def example_5_aggregation(collection):
    """示例5: 聚合查询（统计、分组等）"""
    print("\n" + "=" * 60)
    print("示例5: 聚合查询")
    print("=" * 60)

    # 1. 统计平均持续时间
    avg_duration = collection.aggregate(
        [
            {
                "$group": {
                    "_id": None,
                    "avg_duration": {"$avg": "$duration_days"},
                    "max_duration": {"$max": "$duration_days"},
                    "min_duration": {"$min": "$duration_days"},
                }
            }
        ]
    )
    result = list(avg_duration)[0]
    print(f"持续时间统计:")
    print(f"  平均: {result['avg_duration']:.2f} 天")
    print(f"  最长: {result['max_duration']} 天")
    print(f"  最短: {result['min_duration']} 天")

    # 2. 按流量分组统计
    flow_stats = collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$flow",
                    "count": {"$sum": 1},
                    "avg_duration": {"$avg": "$duration_days"},
                }
            },
            {"$sort": {"count": -1}},
        ]
    )
    print("\n按流量分组统计:")
    for stat in flow_stats:
        print(
            f"  {stat['_id']}: {stat['count']} 条, 平均持续时间 {stat['avg_duration']:.2f} 天"
        )

    # 3. 统计疼痛程度分布
    pain_distribution = collection.aggregate(
        [
            {"$group": {"_id": "$pain_level", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
    )
    print("\n疼痛程度分布:")
    for dist in pain_distribution:
        print(f"  疼痛级别 {dist['_id']}: {dist['count']} 条")


def example_6_update_queries(collection):
    """示例6: 更新操作"""
    print("\n" + "=" * 60)
    print("示例6: 更新操作")
    print("=" * 60)

    # 1. 更新单条记录
    result = collection.update_one(
        {"has_abnormal_discharge": True}, {"$set": {"notes": "已就医检查"}}
    )
    print(f"更新了 {result.modified_count} 条记录")

    # 2. 更新多条记录
    result = collection.update_many(
        {"pain_level": {"$gte": 4}}, {"$set": {"needs_attention": True}}
    )
    print(f"标记了 {result.modified_count} 条需要关注的记录")

    # 3. 使用 $inc 增加数值
    result = collection.update_many(
        {},
        {"$inc": {"view_count": 1}},  # 如果字段不存在会创建
    )
    print(f"为所有记录增加了查看次数")


def example_7_delete_queries(collection):
    """示例7: 删除操作（演示，不实际删除）"""
    print("\n" + "=" * 60)
    print("示例7: 删除操作（仅演示，不实际执行）")
    print("=" * 60)

    # 1. 删除单条记录
    # collection.delete_one({"period_start": "某个日期"})
    print("删除单条: collection.delete_one({条件})")

    # 2. 删除多条记录
    # collection.delete_many({"has_abnormal_discharge": False})
    print("删除多条: collection.delete_many({条件})")

    # 3. 删除所有记录（危险操作）
    # collection.delete_many({})
    print("删除所有: collection.delete_many({})")


def example_8_index_and_performance(collection):
    """示例8: 索引和性能优化"""
    print("\n" + "=" * 60)
    print("示例8: 索引创建")
    print("=" * 60)

    # 创建索引以提高查询性能
    # 单字段索引
    collection.create_index("period_start")
    print("已创建 period_start 索引")

    # 复合索引
    collection.create_index([("duration_days", 1), ("pain_level", -1)])
    print("已创建复合索引 (duration_days, pain_level)")

    # 查看所有索引
    indexes = collection.list_indexes()
    print("\n当前索引:")
    for idx in indexes:
        print(f"  {idx['name']}: {idx.get('key', {})}")


def main():
    """主函数"""
    client, collection = connect_to_mongo()

    try:
        # 检查连接
        count = collection.count_documents({})
        print(f"已连接到 MongoDB，当前有 {count} 条记录\n")

        # 运行各种查询示例
        example_1_basic_find(collection)
        example_2_filter_queries(collection)
        example_3_sort_and_limit(collection)
        example_4_projection(collection)
        example_5_aggregation(collection)
        example_6_update_queries(collection)
        example_7_delete_queries(collection)
        example_8_index_and_performance(collection)

        print("\n" + "=" * 60)
        print("所有示例执行完成！")
        print("=" * 60)

    except Exception as e:
        print(f"执行出错: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
