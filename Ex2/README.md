
题目2：编写MapReduce，统计`/user/hadoop/mapred_dev/ip_time` 中去重后的IP数，越节省性能越好。（35分）

---
      在Map类里定义一个hasset用来接收每次map处理的ip，hasset自动去重。
      建一个cleanup（） 在所有的map运行结束后，对map的数据统一处理。hasset的size()即为去重IP的数量。
      程序运行结果为：1218
运行完之后，描述程序里所做的优化点，每点+5分。
优化点：
     在map阶段就完成数据处理，没有用到reduce。 cleanup对map数据之做一次统一处理，节约资源。
