# GitTest
1hadoop     2leetcode
B:8bit
KB
MB
GB
TB
PB
千进法/1024进法：1KB=1000B, 1KB=1024B; b和B都是byte（字节）的缩写，是最基础的计算机容量单位。

数据规模太大，数据搬迁变得十分困难。Hadoop强调代码向数据迁移，让数据不动，可执行代码移动到数据所在的机器上去。
管道（重用）、消息队列（同步）等数据处理模型。
Mapping、Reducing、Partitioning、Shuffling。
Hadoop在分布式计算与分布式存储中都采用了（master/slave）主/从结构。
一、NameNode          名字节点
-- 运行NameNode消耗大量内存和I/O资源，故NameNode服务器通常不会存储DataNode或TaskTracker；
二、DataNode            数据节点
三、Secondary NameNode  次名字节点
—HDFS元数据的一个快照；
四、JobTracker            作业跟踪节点
—每个hadoop集群只有一个JobTracker，通常在主节点上；
五、TaskTracker           任务跟踪节点

用于监控Hadoop安装的web工具












	

