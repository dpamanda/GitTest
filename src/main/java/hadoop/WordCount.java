package java.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	/**
	 * Hadoop内置的数据类型:
	 * BooleanWritable:标准布尔型数值
	 * ByteWritable:单字节数值
	 * DoubleWritable:双字节数值
	 * FloatWritable:浮点数
	 * IntWritable:整型数
	 * LongWritable:长整型数
	 * Text:使用UTF8格式存储的文本
	 * NullWritable:当<key, value>中的key或value为空时使用
	 *
	 */
	 
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

    	/**
		 * 这个Mapper类是一个泛型类型，它有四个形参类型，分别指定map函数的输入键、输入值、输出键、输出值的类型。
		 * 为什么这里k1要用Object、Text、IntWritable等，而不是java的string啊、int啊类型，
		 * hadoop没有直接使用Java内嵌的类型，而是自己开发了一套可以优化网络序列化传输的基本类型。实现了序列化和反序列化,可以让在节点间传输和通信效率更高
		 * 这些类型都在org.apache.hadoop.io包中。
		 * 比如这个例子中的Object类型，适用于字段需要使用多种类型的时候，Text类型相当于Java中的String类型，IntWritable类型相当于Java中的Integer类型
		 * 定义两个变量或者说是定义两个对象，叫法都可以
		 */

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * context它是mapper的一个内部类，
         * 简单的说顶级接口是为了在map或是reduce任务中跟踪task的状态，很自然的MapContext就是记录了map执行的上下文，
         * 在mapper类中，这个context可以存储一些job conf的信息， 比如job运行时参数等，我们可以在map函数中处理这个信息，这也是Hadoop中参数传递中一个很经典的例子，
         * 同时context作为了map和reduce执行中各个函数的一个桥梁，这个设计和Java web中的session对象、application对象很相似
         */

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());//将Text类型的value转化成字符串类型
            //StringTokenizer是字符串分隔解析类型,StringTokenizer 用来分割字符串，你可以指定分隔符，比如','，或者空格之类的字符。
            /*
             * 1. StringTokenizer(String str) ：构造一个用来解析str的StringTokenizer对象。java默认的分隔符是“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”。
			 * 2. StringTokenizer(String str, String delim) ：构造一个用来解析str的StringTokenizer对象，并提供一个指定的分隔符。
			 *    去掉各种标点符号：StringTokenizer(String str, String " \t\n\r\f,.:;?![]'")
			 * 3. StringTokenizer(String str, String delim, boolean returnDelims) ：构造一个用来解析str的StringTokenizer对象，并提供一个指定的分隔符，同时，指定是否返回分隔符。
             */
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        //我们这里主要用到context的write方法。
        //context起到的是连接map和reduce的桥梁。起到上下文的作用！
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	
    	//Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件。
        Configuration conf = new Configuration();//加载hadoop的配置文件
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        
        //job对象指定了作业执行规范，可以用它来控制整个作业的运行。
        Job job = Job.getInstance(conf, "word count");
        
        /**
         * 在hadoop集群上运行作业的时候，要把代码打包成一个jar文件，
         * 然后把这个文件传到集群上，通过hdoop jar命令来执行这个作业，但是命令中不必指定JAR文件的名称，
         * 在这条命令中通过job对象的setJarByClass（）中传递一个主类就行，hadoop会通过这个主类来查找包含它的JAR文件。
         */
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);//Combiner最终不能影响reduce输出的结果
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        /**
         * TextInputFormat是hadoop默认的输入格式，这个类继承自FileInputFormat,使用这种输入格式，每个文件都会单独作为Map的输入，每行数据都会生成一条记录，每条记录会表示成<key，value>的形式。
         * key的值是每条数据记录在数据分片中的字节偏移量，数据类型是LongWritable.
         * value的值为每行的内容，数据类型为Text。
         * 实际上InputFormat（）是用来生成可供Map处理的<key，value>的。
         * InputSplit是hadoop中用来把输入数据传送给每个单独的Map(也就是我们常说的一个split对应一个Map),
         * InputSplit存储的并非数据本身，而是一个分片长度和一个记录数据位置的数组。
         * 生成InputSplit的方法可以通过InputFormat（）来设置。
         * 当数据传给Map时，Map会将输入分片传送给InputFormat（），InputFormat()则调用getRecordReader()生成RecordReader,RecordReader则再通过creatKey()和creatValue()创建可供Map处理的<key，value>对。
         * 
         */
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        
        /**
         * 只能有一个输出路径，该路径指定的就是reduce函数输出文件的写入目录。
         * 特别注意：输出目录不能提前存在，否则hadoop会报错并拒绝执行作业，这样做的目的是防止数据丢失，因为长时间运行的作业如果结果被意外覆盖掉，那肯定不是我们想要的
         * 默认的输出格式为TextOutputFormat。它和默认输入格式类似，会将每条记录以一行的形式存入文本文件。它的键和值可以是任意形式的，因为程序内部会调用toString()将键和值转化为String类型再输出。
         */
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //使用job.waitForCompletion（）提交作业并等待执行完成，该方法返回一个boolean值，表示执行成功或者失败，这个布尔值被转换成程序退出代码0或1，该布尔参数还是一个详细标识，所以作业会把进度写到控制台。
        //waitForCompletion(）提交作业后，每秒会轮询作业的进度，如果发现和上次报告后有改变，就把进度报告到控制台，作业完成后，如果成功就显示作业计数器，如果失败则把导致作业失败的错误输出到控制台
    }
}