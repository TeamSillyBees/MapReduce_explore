 

# 一、MapReduce分布式计算

## 1.  MapReduce计算模型介绍

### 1.1.   理解MapReduce思想

MapReduce思想在生活中处处可见。或多或少都曾接触过这种思想。MapReduce的思想核心是“**分而治之**”，适用于大量复杂的任务处理场景（大规模数据处理场景）。即使是发布过论文实现分布式计算的谷歌也只是实现了这种思想，而不是自己原创。

**Map****负责“分”**，即把复杂的任务分解为若干个“简单的任务”来并行处理。可以进行拆分的前提是这些小任务可以并行计算，彼此间几乎没有依赖关系。

**Reduce****负责“合”**，即对map阶段的结果进行全局汇总。

这两个阶段合起来正是MapReduce思想的体现。

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image002.jpg)

图：MapReduce思想模型

还有一个比较形象的语言解释MapReduce：　　

我们要数图书馆中的所有书。你数1号书架，我数2号书架。这就是“Map”。我们人越多，数书就更快。

现在我们到一起，把所有人的统计数加在一起。这就是“Reduce”。

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image004.jpg)

 

MapReduce的运行需要由Yarn集群来提供资源调度。

### 1.2.   Hadoop MapReduce设计构思

MapReduce是一个分布式运算程序的编程框架，核心功能是将用户编写的业务逻辑代码和自带默认组件整合成一个完整的分布式运算程序，并发运行在Hadoop的yarn集群上。

既然是做计算的框架，那么表现形式就是有个输入（input），MapReduce操作这个输入（input），通过本身定义好的计算模型，得到一个输出（output）。

对许多开发者来说，自己完完全全实现一个并行计算程序难度太大，而MapReduce就是一种简化并行计算的编程模型，降低了开发并行应用的入门门槛。

Hadoop MapReduce构思体现在如下的三个方面：

- **如何对付大数据处理：分而治之**

对相互间不具有计算依赖关系的大数据，实现并行最自然的办法就是采取分而治之的策略。并行计算的第一个重要问题是如何划分计算任务或者计算数据以便对划分的子任务或数据块同时进行计算。不可分拆的计算任务或相互间有依赖关系的数据无法进行并行计算！

-  **构建抽象模型：Map和Reduce**


MapReduce借鉴了函数式语言中的思想，用Map和Reduce两个函数提供了高层的并行编程抽象模型。

Map: 对一组数据元素进行某种重复式的处理；

Reduce: 对Map的中间结果进行某种进一步的结果整理。

MapReduce中定义了如下的Mapper和Reducer两个抽象的编程接口，由用户去编程实现:

**map: (k1; v1)** **→ [(k2; v2)]**

**reduce: (k2; [v2])** **→ [(k3; v3)]**

Map和Reduce为程序员提供了一个清晰的操作接口抽象描述。通过以上两个编程接口，大家可以看出MapReduce处理的数据类型是<key,value>键值对。

- **统一构架，隐藏系统层细节**

如何提供统一的计算框架，如果没有统一封装底层细节，那么程序员则需要考虑诸如数据存储、划分、分发、结果收集、错误恢复等诸多细节；为此，MapReduce设计并提供了统一的计算框架，为程序员隐藏了绝大多数系统层面的处理细节。

MapReduce最大的亮点在于通过抽象模型和计算框架把需要做什么(what need to do)与具体怎么做(how to do)分开了，为程序员提供一个抽象和高层的编程接口和框架。程序员仅需要关心其应用层的具体计算问题，仅需编写少量的处理应用本身计算问题的程序代码。如何具体完成这个并行计算任务所相关的诸多系统层细节被隐藏起来,交给计算框架去处理：从分布代码的执行，到大到数千小到单个节点集群的自动调度使用。

  ![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image006.jpg)

## 2.  MapReduce编程规范及示例编写

### 2.1.   编程规范

MapReduce 的开发一共有八个步骤, 其中 Map 阶段分为2个步骤，Shuffle 阶段 4 个步骤，Reduce 阶段分为2个步骤

 

**Map阶段2个步骤**

\1. 设置 InputFormat 类, 读取输入文件内容，对输入文件的每一行，解析成key、value对（K1和V1）。

\2. 自定义map方法，每一个键值对调用一次map方法，将第一步的K1和V1结果转换成另外的 Key-Value（K2和V2）对, 输出结果。

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image008.jpg)

 

 

**Shuffle** **阶段** **4** **个步骤**

\3. 对map阶段输出的k2和v2对进行分区

\4. 对不同分区的数据按照相同的Key排序

\5. (可选)对数据进行局部聚合, 降低数据的网络拷贝

\6. 对数据进行分组, 相同Key的Value放入一个集合中,得到K2和[V2]

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image010.jpg)

 

**Reduce** **阶段** **2** **个步骤**

7、对map任务的输出，按照不同的分区，通过网络copy到不同的reduce节点。
 8、对多个map任务的输出进行合并、排序。编写reduce方法，在此方法中将K2和[V2]进行处理，转换成新的key、value(K3和V3)输出，并把reduce的输出保存到文件中。

 

 

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image012.jpg)

### 2.2.   编程步骤:

用户编写的程序分成三个部分：Mapper，Reducer，Driver(提交运行mr程序的客户端)

**Mapper**

(1) 自定义类继承Mapper类

(2) 重写自定义类中的map方法，在该方法中将K1和V1转为K2和V2

(3) 将生成的K2和V2写入上下文中

 

**Reducer**

(1) 自定义类继承Reducer类

(2) 重写Reducer中的reduce方法，在该方法中将K2和[V2]转为K3和V3

(3) 将K3和V3写入上下文中

 

**Driver**

整个程序需要一个Drvier来进行提交，提交的是一个描述了各种必要信息的job对象

（1）定义类，编写main方法

（2）在main方法中指定以下内容:

```
1、创建建一个job任务对象
2、指定job所在的jar包
3、指定源文件的读取方式类和源文件的读取路径
4、指定自定义的Mapper类和K2、V2类型
5、指定自定义分区类（如果有的话）
6、指定自定义Combiner类（如果有的话）
7、指定自定义分组类（如果有的话）
8、指定自定义的Reducer类和K3、V3的数据类型
9、指定输出方式类和结果输出路径
10、将job提交到yarn集群
```



 

### 2.3.   WordCount示例编写

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image014.jpg)

 

需求：在一堆给定的文本文件中统计输出每一个单词出现的总次数

 

**第一步:数据准备**

#####  （这部分为主机上的设置）

\1. 创建一个新的文件

  **cd** **/**export**/**server  **vim** wordcount.txt  

 

\2. 向其中放入以下内容并保存

  hello**,**world**,**hadoop  hive**,**sqoop**,**flume**,**hello  kitty**,**tom**,**jerry**,**world  hadoop  

 

\3. 上传到 HDFS

  hadoop fs  -mkdir -p   /input/wordcount  hadoop fs -put  wordcount.txt /input/wordcount  

 

**第二步:代码编写**

 

**(1）****导入maven坐标**

 

```java
<dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-core</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.25</version>
        </dependency>
    </dependencies>

```

 

 

**(2)****定义一个mapper类**

  

```java
//首先要定义四个泛型的类型
//keyin:  LongWritable    valuein: Text
//keyout: Text            valueout:IntWritable
public class WordCountMapper extends Mapper<LongWritable, Text, Text, Writable>{
	//map方法的生命周期：  框架每传一行数据就被调用一次
	//key :  这一行的起始点在文件中的偏移量
	//value: 这一行的内容
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//拿到一行数据转换为string
		String line = value.toString();
		//将这一行切分出各个单词
		String[] words = line.split(" ");
		//遍历数组，输出<单词，1>
		for(String word:words){
			context.write(new Text(word), new LongWritable (1));
		}
	}
}

```

 

**(2)****定义一个reducer类**

  

```java
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
	//生命周期：框架每传递进来一个kv 组，reduce方法被调用一次
 @Override
 protected void reduce(Text key, Iterable<LongWritable > values, Context context) throws IOException, InterruptedException {
	//定义一个计数器
	int count = 0;
	//遍历这一组kv的所有v，累加到count中
	for(LongWritable value:values){
		count += value.get();
	}
	context.write(key, new LongWritable (count));
 }
}

```

 

**(3)****定义一个Driver主类，用来描述job并提交job**

 

```java
public class WordCountRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、创建建一个job任务对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "wordcount");

        //2、指定job所在的jar包
        job.setJarByClass(WordCountRunner.class);

        //3、指定源文件的读取方式类和源文件的读取路径
        job.setInputFormatClass(TextInputFormat.class); //按照行读取
        //TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/wordcount")); //只需要指定源文件所在的目录即可
         TextInputFormat.addInputPath(job, new Path("file:///E:\\input\\wordcount")); //只需要指定源文件所在的目录即可

        //4、指定自定义的Mapper类和K2、V2类型
        job.setMapperClass(WordCountMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //K2类型
        job.setMapOutputValueClass(LongWritable.class);//V2类型

        //5、指定自定义分区类（如果有的话）
        //6、指定自定义分组类（如果有的话）
        //7、指定自定义的Reducer类和K3、V3的数据类型
        job.setReducerClass(WordCountReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //K3类型
        job.setOutputValueClass(LongWritable.class);  //V3类型

        //8、指定输出方式类和结果输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //TextOutputFormat.setOutputPath(job, new  Path("hdfs://node1:8020/output/wordcount")); //目标目录不能存在，否则报错
        TextOutputFormat.setOutputPath(job, new  Path("file:///E:\\output\\wordcount")); //目标目录不能存在，否则报错

        //9、将job提交到yarn集群
        boolean bl = job.waitForCompletion(true); //true表示可以看到任务的执行进度

        //10.退出执行进程
        System.exit(bl?0:1);
    }
}

```



## 3.  MapReduce程序运行模式

### 3.1.   本地运行模式

（1）mapreduce程序是被提交给LocalJobRunner在本地以单进程的形式运行

（2）而处理的数据及输出结果可以在本地文件系统，也可以在hdfs上

（3）本地模式非常便于进行业务逻辑的调试

### 3.2.   集群运行模式

（1）将mapreduce程序提交给yarn集群，分发到很多的节点上并发执行

（2）处理的数据和输出结果应该位于hdfs文件系统

（3）提交集群的实现步骤：

1、将Driver主类代码中的输入路径和输出路径修改为HDFS路径 

  TextInputFormat**.**addInputPath**(**job**,** **new** Path**(**"hdfs://node1:8020/input/wordcount"**));**  TextOutputFormat**.**setOutputPath**(**job**,** **new** Path**(**"hdfs://node1:8020/output/wordcount"**));**  

2、将程序打成JAR包，然后在集群的任意一个节点上用hadoop命令启动 

  hadoop jar  wordcount.jar cn.itcast.WordCountDriver  

## 4.  深入MapReduce 

### 4.1.   MapReduce的输入和输出

MapReduce框架运转在**<key,value>****键值对**上，也就是说，框架把作业的输入看成是一组<key,value>键值对，同样也产生一组<key,value>键值对作为作业的输出，这两组键值对可能是不同的。

一个MapReduce作业的输入和输出类型如下图所示：可以看出在整个标准的流程中，会有三组<key,value>键值对类型的存在。

 

![http://images.cnitblog.com/blog/381412/201502/121334513709082.png](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image016.jpg)



 

### 4.2.   MapReduce的处理流程解析

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image018.jpg)

 

### 4.3.   Mapper任务执行过程详解

-  **第一阶段**是把输入目录下文件按照一定的标准逐个进行逻辑切片，形成切片规划。默认情况下，Split size = Block size。每一个切片由一个MapTask处理。


​     ![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image020.jpg)

- **第二阶段**是对切片中的数据按照一定的规则解析成<key,value>对。默认规则是把每一行文本内容解析成键值对。key是每一行的起始位置(单位是字节)，value是本行的文本内容。（TextInputFormat）

​      ![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image022.jpg)

- l**第三阶段**是调用Mapper类中的map方法。上阶段中每解析出来的一个<k,v>，调用一次map方法。每次调用map方法会输出零个或多个键值对。


- l**第四阶段**是按照一定的规则对第三阶段输出的键值对进行分区。默认是只有一个区。分区的数量就是Reducer任务运行的数量。默认只有一个Reducer任务。


- l**第五阶段**是对每个分区中的键值对进行排序。首先，按照键进行排序，对于键相同的键值对，按照值进行排序。比如三个键值对<2,2>、<1,3>、<2,1>，键和值分别是整数。那么排序后的结果是<1,3>、<2,1>、<2,2>。如果有第六阶段，那么进入第六阶段；如果没有，直接输出到文件中。


-  **第六阶段**是对数据进行局部聚合处理，也就是combiner处理。键相等的键值对会调用一次reduce方法。经过这一阶段，数据量会减少。**本阶段默认是没有的。**


**
**

 

### 4.4.   Reducer任务执行过程详解

l **第一阶段**是Reducer任务会主动从Mapper任务复制其输出的键值对。Mapper任务可能会有很多，因此Reducer会复制多个Mapper的输出。

l **第二阶段**是把复制到Reducer本地数据，全部进行合并，即把分散的数据合并成一个大的数据。再对合并后的数据排序。

l **第三阶段**是对排序后的键值对调用reduce方法。键相等的键值对调用一次reduce方法，每次调用会产生零个或者多个键值对。最后把这些输出的键值对写入到HDFS文件中。

 

**在整个MapReduce程序的开发过程中，我们最大的工作量是覆盖map方法和覆盖reduce方法。**



 

## 5.  MapReduce分区

 

### 5.1.   分区概述

在 MapReduce 中, 通过我们指定分区, 会将同一个分区的数据发送到同一个Reduce当中进行处理。例如: 为了数据的统计, 可以把一批类似的数据发送到同一个 Reduce 当中, 在同一个 Reduce 当中统计相同类型的数据, 就可以实现类似的数据分区和统计等

其实就是相同类型的数据, 有共性的数据, 送到一起去处理, 在Reduce过程中，可以根据实际需求（比如按某个维度进行归档，类似于数据库的分组），把Map完的数据Reduce到不同的文件中。分区的设置需要与ReduceTaskNum配合使用。比如想要得到5个分区的数据结果。那么就得设置5个ReduceTask。

**需求：将以下数据进行分开处理**

详细数据参见partition.csv 这个文本文件，其中第五个字段表示开奖结果数值，现在需求将15以上的结果以及15以下的结果进行分开成两个文件进行保存

 

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image024.jpg)

 

### 5.2.   分区步骤

**1.** **定义 Mapper**

 

这个 Mapper 程序不做任何逻辑, 也不对 Key-Value 做任何改变, 只是接收数据, 然后往下发送

 

```java
public class MyMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }
}

```

 

**2.** **自定义Partitioner**

 

主要的逻辑就在这里, 这也是这个案例的意义, 通过 Partitioner 将数据分发给不同的 Reducer

  

```java
/**
 * 这里的输入类型与我们map阶段的输出类型相同
 */
public class MyPartitioner extends Partitioner<Text,NullWritable>{
    /**
     * 返回值表示我们的数据要去到哪个分区
     * 返回值只是一个分区的标记，标记所有相同的数据去到指定的分区
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String result = text.toString().split("\t")[5];
        if (Integer.parseInt(result) > 15){
            return 1;
        }else{
            return 0;
        }
    }
}

```

 

**3.** **定义 Reducer 逻辑**

 

这个 Reducer 也不做任何处理, 将数据原封不动的输出即可

  

```java
public class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}

```

 

**4.** **主类中设置分区类和ReduceTask个数** 

 

```java
public class PartitionerRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、创建建一个job任务对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "mypartitioner");

        //2、指定job所在的jar包
        job.setJarByClass(PartitionerRunner.class);

        //3、指定源文件的读取方式类和源文件的读取路径
        job.setInputFormatClass(TextInputFormat.class); //按照行读取
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/partitioner")); //只需要指定源文件所在的目录即可
        // TextInputFormat.addInputPath(job, new Path("file:///E:\\input\\partitioner")); //只需要指定源文件所在的目录即可

        //4、指定自定义的Mapper类和K2、V2类型
        job.setMapperClass(PartitionerMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //K2类型
        job.setMapOutputValueClass(NullWritable.class);//V2类型

        //5、指定自定义分区类（如果有的话）
        job.setPartitionerClass(MyPartitioner.class);
        //6、指定自定义分组类（如果有的话）
        //7、指定自定义的Reducer类和K3、V3的数据类型
        job.setReducerClass(PartitionerReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //K3类型
        job.setOutputValueClass(NullWritable.class);  //V3类型


        //设置Reduce的个数
        job.setNumReduceTasks(2);

        //8、指定输出方式类和结果输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new  Path("hdfs://node1:8020/output/partitioner")); //目标目录不能存在，否则报错
        //TextOutputFormat.setOutputPath(job, new  Path("file:///E:\\output\\partitoner")); //目标目录不能存在，否则报错

        //9、将job提交到yarn集群
        boolean bl = job.waitForCompletion(true); //true表示可以看到任务的执行进度

        //10.退出执行进程
        System.exit(bl?0:1);
    }
}

```



## 6.  MapReduce的排序和序列化

### 6.1.   概述

序列化（Serialization）是指把结构化对象转化为字节流。

反序列化（Deserialization）是序列化的逆过程。把字节流转为结构化对象。

当要在进程间传递对象或持久化对象的时候，就需要序列化对象成字节流，反之当要将接收到或从磁盘读取的字节流转换为对象，就要进行反序列化。

Java的序列化（Serializable）是一个重量级序列化框架，一个对象被序列化后，会附带很多额外的信息（各种校验信息，header，继承体系…），不便于在网络中高效传输；所以，hadoop自己开发了一套序列化机制（**Writable**），精简，高效。不用像java对象类一样传输多层的父子关系，需要哪个属性就传输哪个属性值，大大的减少网络传输的开销。

Writable是Hadoop的序列化格式，hadoop定义了这样一个Writable接口。

一个类要支持可序列化只需实现这个接口即可。

  

```java
public interface  Writable {
 void write(DataOutput out) throws IOException;
 void readFields(DataInput in) throws IOException;
}

```

 

另外 Writable 有一个子接口是 WritableComparable, WritableComparable 是既可实现序列化, 也可以对key进行比较, 我们这里可以通过自定义 Key 实现 WritableComparable 来实现我们的排序功能.

  

```java
 // WritableComparable分别继承Writable和Comparable
public interface WritableComparable<T> extends Writable, Comparable<T> {
}
//Comparable
public interface Comparable<T> {
    int compareTo(T var1);
}

```

 

Comparable接口中的comparaTo方法用来定义排序规则，用于将当前对象与方法的参数进行比较。

例如：o1.compareTo(o2);

如果指定的数与参数相等返回0。

如果指定的数小于参数返回 -1。

如果指定的数大于参数返回 1。

返回正数的话，当前对象（调用compareTo方法的对象o1）要排在比较对象（compareTo传参对象o2）后面，返回负数的话，放在前面。

### 6.2.   需求

数据格式如下

```
  a   1

  a   9

  b   3

  a   7

  b   8

  b   10

  a   5
```

**要求:**

第一列按照字典顺序进行排列

第一列相同的时候, 第二列按照升序进行排列

### 6.3.   分析

**实现自定义的bean来封装数据，并将bean作为map输出的key来传输**

MR程序在处理数据的过程中会对数据排序(map输出的kv对传输到reduce之前，会排序)，排序的依据是map输出的key。所以，我们如果要实现自己需要的排序规则，则可以考虑将排序因素放到key中，让key实现接口：WritableComparable，然后重写key的compareTo方法。

如果自定义的JavaBean要参与MapReduce运算，则必须进行序列化，必须实现**Writable接口，**如果该JavaBean作为K2，则必须实现**WritableComparable接口**，让JavaBean具有排序的功能

### 6.4.   实现

### 6.5.   自定义类型和比较器

  

```java
public class SortBean implements WritableComparable<SortBean>{

  private String word;
  private int  num;

  public String getWord() {
	  return word;
  }

  public void setWord(String word) {
	  this.word = word;
  }

  public int getNum() {
	  return num;
  }

  public void setNum(int num) {
	  this.num = num;
  }

  @Override
  public String toString() {
	  return   word + "\t"+ num ;
  }

  //实现比较器，指定排序的规则
  /*
	规则:
	  第一列(word)按照字典顺序进行排列    //  aac   aad
	  第一列相同的时候, 第二列(num)按照升序进行排列
   */
  /*
	  a  1
	  a  5
	  b  3
	  b  8
   */
  @Override
  public int compareTo(SortBean sortBean) {
	  //先对第一列排序: Word排序
	  int result = this.word.compareTo(sortBean.word);
	  //如果第一列相同，则按照第二列进行排序
	  if(result == 0){
		  return  this.num - sortBean.num;
	  }
	  return result;
  }

  //实现序列化
  @Override
  public void write(DataOutput out) throws IOException {
	  out.writeUTF(word);
	  out.writeInt(num);
  }

  //实现反序列
  @Override
  public void readFields(DataInput in) throws IOException {
		  this.word = in.readUTF();
		  this.num = in.readInt();
  }
}

```

 

### 6.6.   编写Mapper代码

 

```java
public class SortMapper extends Mapper<LongWritable,Text,SortBean,NullWritable> {
  /*
	map方法将K1和V1转为K2和V2:

	K1            V1
	0            a  3
	5            b  7
	----------------------
	K2                         V2
	SortBean(a  3)         NullWritable
	SortBean(b  7)         NullWritable
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	  //1:将行文本数据(V1)拆分，并将数据封装到SortBean对象,就可以得到K2
	  String[] split = value.toString().split("\t");

	  SortBean sortBean = new SortBean();
	  sortBean.setWord(split[0]);
	  sortBean.setNum(Integer.parseInt(split[1]));

	  //2:将K2和V2写入上下文中
	  context.write(sortBean, NullWritable.get());
  }
 }

```

 

### 6.7.   编写Reducer代码

```java
public class SortReducer extends Reducer<SortBean,NullWritable,SortBean,NullWritable> {
 
   //reduce方法将新的K2和V2转为K3和V3
   @Override
   protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
   }
}

```



### 6.8.   编写主类代码

​     

```java
public class SortRunner {
      public static void main(String[] args) throws Exception {
		  
		  Configuration conf = new Configuration();
          //1:创建job对象
          Job job = Job.getInstance(conf, "mapreduce_sort");
   
       
	       //2:指定job所在的jar包
		  job.setJarByClass(SortRunner.class);
    
		  //3:指定源文件的读取方式类和源文件的读取路径
		  job.setInputFormatClass(TextInputFormat.class);
		  ///TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/input/sort_input"));
		  TextInputFormat.addInputPath(job, new Path("file:///D:\\input\\sort_input"));

		  //4:指定自定义的Mapper类和K2、V2类型
		  job.setMapperClass(SortMapper.class);
		  job.setMapOutputKeyClass(SortBean.class);
		  job.setMapOutputValueClass(NullWritable.class);


		  //5:指定自定义的Reducer类和K3、V3的数据类型
		  job.setReducerClass(SortReducer.class);
		  job.setOutputKeyClass(SortBean.class);
		  job.setOutputValueClass(NullWritable.class);


		  //6:指定输出方式类和结果输出路径
		  job.setOutputFormatClass(TextOutputFormat.class);
		  TextOutputFormat.setOutputPath(job, new Path("file:///D:\\out\\sort_out"));

   
         //7:将job提交给yarn集群
         boolean bl = job.waitForCompletion(true);

         System.exit(bl?0:1);
      }
  }

```



## 7.  MapReuce的Combineer

### 7.1.   概念

每一个 map 都可能会产生大量的本地输出，Combiner 的作用就是对 map 端的输出先做一次合并，以减少在 map 和 reduce 节点之间的数据传输量，以提高网络IO 性能，是 MapReduce 的一种优化手段之一

- combiner 是 MR 程序中 Mapper 和 Reducer 之外的一种组件

- combiner 组件的父类就是 Reducer

- combiner 和 reducer 的区别在于运行的位置
  -  combiner 是在每一个 maptask 所在的节点运行
  - Reducer 是接收全局所有 Mapper 的输出结果
  - combiner 的意义就是对每一个 maptask 的输出进行局部汇总，以减小网络传输量

 

### 7.2.   实现步骤

在这里以单词统计为例,实现Combiner

1、自定义一个 combiner 继承 Reducer，重写 reduce 方法

  

```java
public class MyCombiner extends Reducer<Text,LongWritable,Text,LongWritable> {


    /*
       key : hello
       values: <1,1,1,1>
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        //1:遍历集合，将集合中的数字相加，得到 V3
        for (LongWritable value : values) {
            count += value.get();
        }
        //2:将K3和V3写入上下文中
        context.write(key, new LongWritable(count));
    }
}

```

2、`在 job 中设置 `job.setCombinerClass(CustomCombiner.class)

```java
job.setCombinerClass(MyCombiner.class);
```

combiner 能够应用的前提是不能影响最终的业务逻辑，而且，combiner 的输出 kv 应该跟 reducer 的输入 kv 类型要对应起来

3、对使用Combiner之前和之后的日志进行对比

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image026.jpg)

通过对比发现，使用Combiner之后，Reduce输入的键值对数量降低了，提供了网络传输效率。

## 8.  MapReduce的自定义分组

GroupingComparator是mapreduce当中reduce端的一个功能组件，主要的作用是决定哪些数据作为一组，调用一次reduce的逻辑，默认是每个不同的key，作为多个不同的组，每个组调用一次reduce逻辑，我们可以自定义GroupingComparator实现不同的key作为同一个组，调用一次reduce逻辑

### 8.1.   需求

有如下订单数据

| 订单id        | 商品id | 成交金额 |
| ------------- | ------ | -------- |
| Order_0000001 | Pdt_01 | 222.8    |
| Order_0000001 | Pdt_05 | 25.8     |
| Order_0000002 | Pdt_03 | 522.8    |
| Order_0000002 | Pdt_04 | 122.4    |
| Order_0000002 | Pdt_05 | 722.4    |
| Order_0000003 | Pdt_01 | 222.8    |

 

现在需要求出每一个订单中成交金额最大的一笔交易

 

### 8.2.   分析

1、利用“订单id和成交金额”作为key，可以将map阶段读取到的所有订单数据按照id分区，按照金额排序，发送到reduce

2、在reduce端利用groupingcomparator将订单id相同的kv聚合成组，然后取第一个即是最大值

### 8.3.   实现

#### 8.3.1.  第一步：定义OrderBean

定义一个OrderBean，里面定义两个字段，第一个字段是我们的orderId，第二个字段是我们的金额（注意金额一定要使用Double或者DoubleWritable类型，否则没法按照金额顺序排序）

 

 

```java
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;
    @Override
    public int compareTo(OrderBean o) {
        //比较订单id的排序顺序
        int i = this.orderId.compareTo(o.orderId);
        if(i==0){
          //如果订单id相同，则比较金额，金额大的排在前面
           i = - this.price.compareTo(o.price);
        }
        return i;
    }
    @Override
    public void write(DataOutput out) throws IOException {
            out.writeUTF(orderId);
            out.writeDouble(price);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId =  in.readUTF();
        this.price = in.readDouble();
    }
    public OrderBean() {
    }
    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }
    public String getOrderId() {
        return orderId;
    }
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    public Double getPrice() {
        return price;
    }
    public void setPrice(Double price) {
        this.price = price;
    }
    @Override
    public String toString() {
        return  orderId +"\t"+price;
    }
}

```

 

#### 8.3.2.  第二步：自定义分区

自定义分区，按照订单id进行分区，把所有订单id相同的数据，都发送到同一个reduce中去

  

```java
public class OrderPartition extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        //自定义分区，将相同订单id的数据发送到同一个reduce里面去
        return  (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE)%i;
    }
}

```

 

#### 8.3.3.  第三步：自定义groupingComparator

按照我们自己的逻辑进行分组，通过比较相同的订单id，将相同的订单id放到一个组里面去，进过分组之后当中的数据，已经全部是排好序的数据，我们只需要取前topN即可

 

```java
/*

  1: 继承WriteableComparator
  2: 调用父类的有参构造
  3: 指定分组的规则(重写方法)
 */

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// 1: 继承WriteableComparator
public class OrderGroupComparator extends WritableComparator {
    // 2: 调用父类的有参构造
    public OrderGroupComparator() {
        super(OrderBean.class,true);
    }

    //3: 指定分组的规则(重写方法)
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //3.1 对形参做强制类型转换
        OrderBean first = (OrderBean)a;
        OrderBean second = (OrderBean)b;

        //3.2 指定分组规则
        return first.getOrderId().compareTo(second.getOrderId());
    }
}

```

 

#### 8.3.4.  第四步：程序main函数入口

  

```java
public class GroupingRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、创建建一个job任务对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "grouping_demo");

        //2、指定job所在的jar包
        job.setJarByClass(GroupingRunner.class);

        //3、指定源文件的读取方式类和源文件的读取路径
        job.setInputFormatClass(TextInputFormat.class); //按照行读取
        //TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/wordcount")); //只需要指定源文件所在的目录即可
        TextInputFormat.addInputPath(job, new Path("file:///E:\\input\\grouping_demo")); //只需要指定源文件所在的目录即可

        //4、指定自定义的Mapper类和K2、V2类型
        job.setMapperClass(GroupingMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(OrderBean.class); //K2类型
        job.setMapOutputValueClass(Text.class);//V2类型

        //5、指定自定义分区类（如果有的话）
        job.setPartitionerClass(MyPartitioner.class);
        //6、指定自定义分组类（如果有的话）
        job.setGroupingComparatorClass(GroupingComparator.class);
        //7、指定自定义Combiner类(如果有的话)
        //job.setCombinerClass(MyCombiner.class);


        //设置ReduceTask个数
        job.setNumReduceTasks(3);

        //8、指定自定义的Reducer类和K3、V3的数据类型
        job.setReducerClass(GroupingReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //K3类型
        job.setOutputValueClass(NullWritable.class);  //V3类型

        //9、指定输出方式类和结果输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //TextOutputFormat.setOutputPath(job, new  Path("hdfs://node1:8020/output/wordcount")); //目标目录不能存在，否则报错
        TextOutputFormat.setOutputPath(job, new  Path("file:///E:\\output\\grouping_demo")); //目标目录不能存在，否则报错

        //10、将job提交到yarn集群
        boolean bl = job.waitForCompletion(true); //true表示可以看到任务的执行进度

        //11.退出执行进程
        System.exit(bl?0:1);
    }
}

```

 

 

 

## 9.  MapReduce的运行机制详解

 ![未命名文件 (1)](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image028.jpg)

### 9.1.   MapTask工作机制

简单概述：inputFile通过split被逻辑切分为多个split文件，通过Record按行读取内容给map（用户自己实现的）进行处理，数据被map处理结束之后交给OutputCollector收集器，对其结果key进行分区（默认使用hash分区），然后写入buffer，每个map task都有一个内存缓冲区，存储着map的输出结果，当缓冲区快满的时候需要将缓冲区的数据以一个临时文件的方式存放到磁盘，当整个map task结束后再对磁盘中这个map task产生的所有临时文件做合并，生成最终的正式输出文件，然后等待reduce task来拉数据。

 

#### 9.1.1.  详细步骤

1、 首先，读取数据组件InputFormat（默认TextInputFormat）会通过getSplits方法对输入目录中文件进行逻辑切片规划得到splits，有多少个split就对应启动多少个MapTask。split与block的对应关系默认是一对一。

2、 将输入文件切分为splits之后，由RecordReader对象（默认LineRecordReader）进行读取，以\n作为分隔符，读取一行数据，返回<key，value>。Key表示每行首字符偏移值，value表示这一行文本内容。

3、 读取split返回<key,value>，进入用户自己继承的Mapper类中，执行用户重写的map函数。RecordReader读取一行这里调用一次。

4、 map逻辑完之后，将map的每条结果通过context.write进行collect数据收集。在collect中，会先对其进行分区处理，默认使用HashPartitioner。

MapReduce 提供 Partitioner 接口, 它的作用就是根据 Key 或 Value 及 Reducer 的数量来决定当前的这对输出数据最终应该交由哪个 Reduce task 处理, 默认对 Key Hash 后再以 Reducer 数量取模. 默认的取模方式只是为了平均 Reducer 的处理能力, 如果用户自己对 Partitioner 有需求, 可以订制并设置到 Job 上

5、接下来, 会将数据写入内存, 内存中这片区域叫做环形缓冲区, 缓冲区的作用是批量收集 Mapper 结果, 减少磁盘IO的影响. 我们的 Key/Value 对以及 Partition 的结果都会被写入缓冲区. 当然, 写入之前，Key 与 Value 值都会被序列化成字节数组

环形缓冲区其实是一个数组, 数组中存放着 Key, Value 的序列化数据和 Key, Value 的元数据信息, 包括 Partition, Key 的起始位置, Value 的起始位置以及 Value 的长度. 环形结构是一个抽象概念

缓冲区是有大小限制, 默认是 100MB. 当 Mapper 的输出结果很多时, 就可能会撑爆内存, 所以需要在一定条件下将缓冲区中的数据临时写入磁盘, 然后重新利用这块缓冲区. 这个从内存往磁盘写数据的过程被称为 Spill, 中文可译为溢写. 这个溢写是由单独线程来完成, 不影响往缓冲区写 Mapper 结果的线程. 溢写线程启动时不应该阻止 Mapper 的结果输出, 所以整个缓冲区有个溢写的比例 spill.percent. 这个比例默认是 0.8, 也就是当缓冲区的数据已经达到阈值 buffer size * spill percent = 100MB * 0.8 = 80MB, 溢写线程启动, 锁定这 80MB 的内存, 执行溢写过程. Mapper 的输出结果还可以往剩下的 20MB 内存中写, 互不影响

6、当溢写线程启动后, 需要**对这 80MB 空间内的 Key 做排序 (Sort)**. 排序是 MapReduce 模型默认的行为, 这里的排序也是对序列化的字节做的排序

如果 Job 设置过 Combiner, 那么现在就是使用 Combiner 的时候了. 将有相同 Key 的 Key/Value 对的 Value 加起来, 减少溢写到磁盘的数据量. Combiner 会优化 MapReduce 的中间结果, 所以它在整个模型中会多次使用

7、合并溢写文件, 每次溢写会在磁盘上生成一个临时文件 (写之前判断是否有 Combiner), 如果 Mapper 的输出结果真的很大, 有多次这样的溢写发生, 磁盘上相应的就会有多个临时文件存在. 当整个数据处理结束之后开始对磁盘中的临时文件进行 Merge 合并, 因为最终的文件只有一个, 写入磁盘, 并且为这个文件提供了一个索引文件, 以记录每个reduce对应数据的偏移量

#### 9.1.2.  配置

| **配置**                           | **默认值**                       | **解释**                   |
| ---------------------------------- | -------------------------------- | -------------------------- |
| `mapreduce.task.io.sort.mb`        | 100                              | 设置环型缓冲区的内存值大小 |
| `mapreduce.map.sort.spill.percent` | 0.8                              | 设置溢写的比例             |
| `mapreduce.cluster.local.dir`      | `${hadoop.tmp.dir}/mapred/local` | 溢写数据目录               |
| `mapreduce.task.io.sort.factor`    | 10                               | 设置一次合并多少个溢写文件 |

### 9.2.   ReduceTask工作机制

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image030.jpg)

Reduce大致分为copy、sort、reduce三个阶段，重点在前两个阶段。copy阶段包含一个eventFetcher来获取已完成的map列表，由Fetcher线程去copy数据，在此过程中会启动两个merge线程，分别为inMemoryMerger和onDiskMerger，分别将内存中的数据merge到磁盘和将磁盘中的数据进行merge。待数据copy完成之后，copy阶段就完成了，开始进行sort阶段，sort阶段主要是执行finalMerge操作，纯粹的sort阶段，完成之后就是reduce阶段，调用用户定义的reduce函数进行处理。

详细步骤：

1、**Copy阶段，**简单地拉取数据。Reduce进程启动一些数据copy线程(Fetcher)，通过HTTP方式请求maptask获取属于自己的文件。

2、**Merge阶段，**这里的merge如map端的merge动作，只是数组中存放的是不同map端copy来的数值。Copy过来的数据会先放入内存缓冲区中，这里的缓冲区大小要比map端的更为灵活。merge有三种形式：内存到内存；内存到磁盘；磁盘到磁盘。默认情况下第一种形式不启用。当内存中的数据量到达一定阈值，就启动内存到磁盘的merge。与map 端类似，这也是溢写的过程，然后在磁盘中生成了众多的溢写文件。第二种merge方式一直在运行，直到没有map端的数据时才结束，然后启动第三种磁盘到磁盘的merge方式生成最终的文件。

3、**合并排序，**把分散的数据合并成一个大的数据后，还会再对合并后的数据排序。

4、**对排序后的键值对调用reduce方法，**键相等的键值对调用一次reduce方法，每次调用会产生零个或者多个键值对，最后把这些输出的键值对写入到HDFS文件中。

### 9.3.   MapReduce的shuffle过程

map阶段处理的数据如何传递给reduce阶段，是MapReduce框架中最关键的一个流程，这个流程就叫shuffle。

shuffle: 洗牌、发牌——（核心机制：数据分区，排序，Combiner，分组等过程）。

![IMG_256](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image032.jpg)

 

shuffle是Mapreduce的核心，它分布在Mapreduce的map阶段和reduce阶段。一般把从Map产生输出开始到Reduce取得数据作为输入之前的过程称作shuffle。

1）**Collect****阶段**：将MapTask的结果输出到默认大小为100M的环形缓冲区，保存的是key/value，Partition分区信息等。

2）**Spill****阶段：**当内存中的数据量达到一定的阀值的时候，就会将数据写入本地磁盘，在将数据写入磁盘之前需要对数据进行一次排序的操作，如果配置了combiner，还会将有相同分区号和key的数据进行排序。

3）**Merge****阶段：**把所有溢出的临时文件进行一次合并操作，以确保一个MapTask最终只产生一个中间数据文件。

4）**Copy****阶段：**ReduceTask启动Fetcher线程到已经完成MapTask的节点上复制一份属于自己的数据，这些数据默认会保存在内存的缓冲区中，当内存的缓冲区达到一定的阀值的时候，就会将数据写到磁盘之上。

5）**Merge****阶段**：在ReduceTask远程复制数据的同时，会在后台开启两个线程对内存到本地的数据文件进行合并操作。

6）**Sort****阶段**：在对数据进行合并的同时，会进行排序操作，由于MapTask阶段已经对数据进行了局部的排序，ReduceTask只需保证Copy的数据的最终整体有效性即可。

Shuffle中的缓冲区大小会影响到mapreduce程序的执行效率，原则上说，缓冲区越大，磁盘io的次数越少，执行速度就越快

缓冲区的大小可以通过参数调整, 参数：mapreduce.task.io.sort.mb 默认100M

 

 

## 10.  MapReduce高阶训练

### 10.1.  上网流量统计

数据格式如下:

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image034.jpg)

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image036.jpg)

 

#### 10.1.1.    需求一：统计求和

统计每个手机号的上行数据包数总和，下行数据包数总和，上行总流量之和，下行总流量之和

分析：以手机号码作为key值，上行数据包，下行数据包，上行总流量，下行总流量四个字段作为value值，然后以这个key和value作为map阶段的输出，reduce阶段的输入。

##### 10.1.1.1. 思路分析

![3-流量统计求和](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image038.jpg)

##### 10.1.1.2. 代码实现

**第一步：自定义map的输出value对象FlowBean**

  

```java
ublic class FlowBean implements Writable {
    private Integer upFlow;
    private Integer  downFlow;
    private Integer upCountFlow;
    private Integer downCountFlow;
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(upCountFlow);
        out.writeInt(downCountFlow);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.upCountFlow = in.readInt();
        this.downCountFlow = in.readInt();
    }
    public FlowBean() {
    }
    public FlowBean(Integer upFlow, Integer downFlow, Integer upCountFlow, Integer downCountFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.upCountFlow = upCountFlow;
        this.downCountFlow = downCountFlow;
    }
    public Integer getUpFlow() {
        return upFlow;
    }
    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }
    public Integer getDownFlow() {
        return downFlow;
    }
    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }
    public Integer getUpCountFlow() {
        return upCountFlow;
    }
    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }
    public Integer getDownCountFlow() {
        return downCountFlow;
    }
    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }
    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", upCountFlow=" + upCountFlow +
                ", downCountFlow=" + downCountFlow +
                '}';
    }
}

```

**第二步：定义FlowMapper类**

  

```java
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));
        context.write(new Text(split[1]),flowBean);
    }
}

```

 

**第三步：定义FlowReducer类**

  

```java
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
         Integer upFlow = 0;
         Integer  downFlow = 0;
         Integer upCountFlow = 0;
         Integer downCountFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        context.write(key,flowBean);
    }
}

```

 

**第四步：程序main函数入口FlowMain**

```java
public class FlowBeanMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), FlowBeanMain.class.getSimpleName());
        job.setJarByClass(FlowBeanMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\flowcount\\input"));
        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\flowcount\\out"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new FlowBeanMain(),args);
    }
}

```



#### 

 

## 11.  MapReduce并行度机制

### 11.1.  MapTask并行度机制

MapTask的并行度指的是map阶段有多少个并行的task共同处理任务。map阶段的任务处理并行度，势必影响到整个job的处理速度。那么，MapTask并行实例是否越多越好呢？其并行度又是如何决定呢？

一个MapReducejob的**map****阶段并行度由客户端在提交job时决定**，即客户端提交job之前会对待处理数据进行**逻辑切片**。切片完成会形成**切片规划文件（job.split）**，每个逻辑切片最终对应启动一个maptask。

逻辑切片机制由FileInputFormat实现类的**getSplits()**方法完成。

#### 11.1.1.    FileInputFormat切片机制

**FileInputFormat****中默认的切片机制：**

l 切片大小，默认等于block大小，即128M

l block是HDFS上物理上存储的存储的数据，切片是MapReduce对数据逻辑上的划分。

l 在FileInputFormat中，计算切片大小的逻辑：

  Math**.**max**(**minSize**,** Math**.**min**(**maxSize**,** blockSize**));**   

 

**FileInputFormat****中切片的大小的由这几个值来运算决定：**

在 FileInputFormat 中，计算切片大小的逻辑：

long splitSize = computeSplitSize(blockSize, minSize, maxSize)，

切片主要由这几个值来运算决定：

**blocksize****：**默认是 128M，可通过 dfs.blocksize 修改

**minSize****：**默认是 1，可通过 mapreduce.input.fileinputformat.split.minsize 修改

**maxsize****：**默认是 Long.MaxValue，可通过 mapreduce.input.fileinputformat.split.maxsize 修改

如果设置的最大值maxsize比blocksize值小，则按照maxSize切数据

如果设置的最小值minsize比blocksize值大，则按照minSize切数据

 

 

整个切片的核心过程在getSplit()方法中完成。

数据切片只是在逻辑上对输入数据进行分片，并不会再磁盘上将其切分成分片进行存储。InputSplit只记录了分片的元数据信息，比如起始位置、长度以及所在的节点列表等。

 

### 11.2.  Reducetask并行度机制

reducetask并行度同样影响整个job的执行并发度和执行效率，与maptask的并发数由切片数决定不同，Reducetask数量的决定是可以直接手动设置：

  job**.**setNumReduceTasks**(**4**);**  

如果数据分布不均匀，就有可能在reduce阶段产生数据倾斜。

注意： reducetask数量并不是任意设置，还要考虑业务逻辑需求，有些情况下，需要计算全局汇总结果，就只能有1个reducetask。

## 12.  MapReduce性能优化策略

使用Hadoop进行大数据运算，当数据量极其大时，那么对MapReduce性能的调优重要性不言而喻，尤其是Shuffle过程中的参数配置对作业的总执行时间影响特别大。下面总结一些和MapReduce相关的性能调优方法，主要从五个方面考虑：数据输入、Map阶段、Reduce阶段、Shuffle阶段和其他调优属性。

1．数据输入

在执行MapReduce任务前，将小文件进行合并，大量的小文件会产生大量的map任务，增大map任务装载的次数，而任务的装载比较耗时，从而导致MapReduce运行速度较慢。

2．Map阶段

（1）减少溢写（spill）次数：通过调整io.sort.mb及sort.spill.percent参数值，增大触发spill的内存上限，减少spill次数，从而减少磁盘IO。

（2）减少合并（merge）次数：通过调整io.sort.factor参数，增大merge的文件数目，减少merge的次数，从而缩短mr处理时间。

（3）在map之后，不影响业务逻辑前提下，先进行combine处理，减少 I/O。

我们在上面提到的那些属性参数，都是位于mapred-size.xml文件中，这些属性参数的调优方式如表所示。

Map阶段调优属性

| **属性名称**                              | **类型** | **默认值** | **说明**                                                     |
| ----------------------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapreduce.task.io.sort.mb                 | int      | 100        | 配置排序map输出时使用的内存缓冲区的大小，默认100Mb，实际开发中可以设置大一些。 |
| mapreduce.map.sort.spill.percent          | float    | 0.80       | map输出内存缓冲和用来开始磁盘溢出写过程的记录边界索引的阈值，即最大使用环形缓冲内存的阈值。一般默认是80%。也可以直接设置为100% |
| mapreduce.task.io.sort.factor             | int      | 10         | 排序文件时，一次最多合并的流数，实际开发中可将这个值设置为100。 |
| mapreduce.task.min.num.spills.for.combine | int      | 3          | 运行combiner时，所需的最少溢出文件数(如果已指定combiner)     |

 

3．Reduce阶段

（1）合理设置map和reduce数：两个都不能设置太少，也不能设置太多。太少，会导致task等待，延长处理时间；太多，会导致 map、reduce任务间竞争资源，造成处理超时等错误。

（2）设置map、reduce共存：调整slowstart.completedmaps参数，使map运行到一定程度后，reduce也开始运行，减少reduce的等待时间。

（3）规避使用reduce：因为reduce在用于连接数据集的时候将会产生大量的网络消耗。通过将MapReduce参数setNumReduceTasks设置为0来创建一个只有map的作业。

（4）合理设置reduce端的buffer：默认情况下，数据达到一个阈值的时候，buffer中的数据就会写入磁盘，然后reduce会从磁盘中获得所有的数据。也就是说，buffer和reduce是没有直接关联的，中间多一个写磁盘->读磁盘的过程，既然有这个弊端，那么就可以通过参数来配置，使得buffer中的一部分数据可以直接输送到reduce，从而减少IO开销。这样一来，设置buffer需要内存，读取数据需要内存，reduce计算也要内存，所以要根据作业的运行情况进行调整。

我们在上面提到的属性参数，都是位于mapred-size.xml文件中，这些属性参数的调优方式如表所示。

Reduce阶段的调优属性

| **属性名称**                                 | **类型** | **默认值** | **说明**                                                     |
| -------------------------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapreduce.job.reduce.slowstart.completedmaps | float    | 0.05       | 当map task在执行到5%，就开始为reduce申请资源。开始执行reduce操作，reduce可以开始拷贝map结果数据和做reduce shuffle操作。 |
| mapred.job.reduce.input.buffer.percent       | float    | 0.0        | 在reduce过程，内存中保存map输出的空间占整个堆空间的比例。如果reducer需要的内存较少，可以增加这个值来最小化访问磁盘的次数。 |

 

4．Shuffle阶段

Shuffle阶段的调优就是给Shuffle过程尽量多地提供内存空间，以防止出现内存溢出现象，可以由参数mapred.child.java.opts来设置，任务节点上的内存大小应尽量大。

我们在上面提到的属性参数，都是位于mapred-site.xml文件中，这些属性参数的调优方式如表所示。

​                                                                      表4-1  shuffle阶段的调优属性

| **属性名称**                  | **类型** | **默认值** | **说明**                                                     |
| ----------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapred.map.child.java.opts    |          | -Xmx200m   | map Task使用的的内存设置，默认情况下，-Xmx都是配置200m的，但是在实际情况下，这个显然是不够用的，一般设置为-Xmx1024m |
| mapred.reduce.child.java.opts |          | -Xmx200m   | reduce Task使用的的内存设置，默认情况下，-Xmx都是配置200m的，但是在实际情况下，这个显然是不够用的，一般设置为-Xmx1024m |

 

5．其他调优属性

除此之外，MapReduce还有一些基本的资源属性的配置，这些配置的相关参数都位于mapred-default.xml文件中，我们可以合理配置这些属性提高MapReduce性能，表4-4列举了部分调优属性。

​                                                                    表4-2  MapReduce资源调优属性

| **属性名称**                            | **类型** | **默认值** | **说明**                                                     |
| --------------------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapreduce.map.memory.mb                 | int      | 1024       | 一个Map Task可使用的资源上限。如果Map Task实际使用的资源量超过该值，则会被强制杀死。 |
| mapreduce.reduce.memory.mb              | int      | 1024       | 一个Reduce Task可使用的资源上限。如果Reduce  Task实际使用的资源量超过该值，则会被强制杀死。 |
| mapreduce.map.cpu.vcores                | int      | 1          | 每个Map task可使用的最多cpu core数目                         |
| mapreduce.reduce.cpu.vcores             | int      | 1          | 每个Reduce task可使用的最多cpu core数目                      |
| mapreduce.reduce.shuffle.parallelcopies | int      | 5          | 每个reduce去map中拿数据的并行数。                            |
| mapreduce.map.maxattempts               | int      | 4          | 每个Map Task最大重试次数，一旦重试参数超过该值，则认为Map Task运行失败 |
| mapreduce.reduce.maxattempts            | int      | 4          | 每个Reduce Task最大重试次数，一旦重试参数超过该值，则认为Map Task运行失败 |

 

 

# 二、Apache Hadoop YARN

## 1.  Yarn通俗介绍

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image042.jpg)

Apache Hadoop YARN （Yet Another Resource Negotiator，另一种资源协调者）是一种新的 Hadoop 资源管理器，它是一个通用资源管理系统和调度平台，可为上层应用提供统一的资源管理和调度，它的引入为集群在利用率、资源统一管理和数据共享等方面带来了巨大好处。

可以把yarn理解为相当于一个分布式的操作系统平台，而mapreduce等运算程序则相当于运行于操作系统之上的应用程序，Yarn为这些程序提供运算所需的资源（内存、cpu）。

 

l yarn并不清楚用户提交的程序的运行机制

l yarn只提供运算资源的调度（用户程序向yarn申请资源，yarn就负责分配资源）

l yarn中的主管角色叫ResourceManager

l yarn中具体提供运算资源的角色叫NodeManager

l yarn与运行的用户程序完全解耦，意味着yarn上可以运行各种类型的分布式运算程序，比如mapreduce、storm，spark，tez ……

l spark、storm等运算框架都可以整合在yarn上运行，只要他们各自的框架中有符合yarn规范的资源请求机制即可

l yarn成为一个通用的资源调度平台.企业中以前存在的各种运算集群都可以整合在一个物理集群上，提高资源利用率，方便数据共享

## 2.    Yarn基本架构

YARN是一个资源管理、任务调度的框架，主要包含三大模块：ResourceManager（RM）、NodeManager（NM）、ApplicationMaster（AM）。

ResourceManager负责所有资源的监控、分配和管理；

ApplicationMaster负责每一个具体应用程序的调度和协调；

NodeManager负责每一个节点的维护。

对于所有的applications，RM拥有绝对的控制权和对资源的分配权。而每个AM则会和RM协商资源，同时和NodeManager通信来执行和监控task。

## 3.  Yarn三大组件介绍

### 3.1.   ResourceManager

l ResourceManager负责整个集群的资源管理和分配，是一个全局的资源管理系统。

l NodeManager以心跳的方式向ResourceManager汇报资源使用情况（目前主要是CPU和内存的使用情况）。RM只接受NM的资源回报信息，对于具体的资源处理则交给NM自己处理。

l YARN Scheduler根据application的请求为其分配资源，不负责application job的监控、追踪、运行状态反馈、启动等工作。

### 3.2.   NodeManager

l NodeManager是每个节点上的资源和任务管理器，它是管理这台机器的代理，负责该节点程序的运行，以及该节点资源的管理和监控。YARN集群每个节点都运行一个NodeManager。

l NodeManager定时向ResourceManager汇报本节点资源（CPU、内存）的使用情况和Container的运行状态。当ResourceManager宕机时NodeManager自动连接RM备用节点。

l NodeManager接收并处理来自ApplicationMaster的Container启动、停止等各种请求。

### 3.3.   ApplicationMaster

l 用户提交的每个应用程序均包含一个ApplicationMaster，它可以运行在ResourceManager以外的机器上。

l 负责与RM调度器协商以获取资源（用Container表示）。

l 将得到的任务进一步分配给内部的任务(资源的二次分配)。

l 与NM通信以启动/停止任务。

l 监控所有任务运行状态，并在任务运行失败时重新为任务申请资源以重启任务。

## 4.  Yarn运行流程

l client向RM提交应用程序，其中包括启动该应用的ApplicationMaster的必须信息，例如ApplicationMaster程序、启动ApplicationMaster的命令、用户程序等。

l ResourceManager启动一个container用于运行ApplicationMaster。

l 启动中的ApplicationMaster向ResourceManager注册自己，启动成功后与RM保持心跳。

l ApplicationMaster向ResourceManager发送请求，申请相应数目的container。

l ResourceManager返回ApplicationMaster的申请的containers信息。申请成功的container，由ApplicationMaster进行初始化。container的启动信息初始化后，AM与对应的NodeManager通信，要求NM启动container。AM与NM保持心跳，从而对NM上运行的任务进行监控和管理。

l container运行期间，ApplicationMaster对container进行监控。container通过RPC协议向对应的AM汇报自己的进度和状态等信息。

l 应用运行期间，client直接与AM通信获取应用的状态、进度更新等信息。

l 应用运行结束后，ApplicationMaster向ResourceManager注销自己，并允许属于它的container被收回。



![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image045.jpg)

![4-yarn的工作流程](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image047.jpg)

## 5.  Yarn的调度器Scheduler

理想情况下，我们应用对Yarn资源的请求应该立刻得到满足，但现实情况资源往往是有限的，特别是在一个很繁忙的集群，一个应用资源的请求经常需要等待一段时间才能的到相应的资源。在**Yarn****中，负责给应用分配资源的就是Scheduler**。其实调度本身就是一个难题，很难找到一个完美的策略可以解决所有的应用场景。为此，Yarn提供了多种调度器和可配置的策略供我们选择。

在Yarn中有三种调度器可以选择：FIFO Scheduler ，Capacity Scheduler，Fair Scheduler。

### 5.1.   FIFO Scheduler

**FIFO** Scheduler把应用按提交的顺序排成一个队列，这是一个**先进先出**队列，在进行资源分配的时候，先给队列中最头上的应用进行分配资源，待最头上的应用需求满足后再给下一个分配，以此类推。

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image049.jpg)

FIFO Scheduler是最简单也是最容易理解的调度器，也不需要任何配置，但它并不适用于共享集群。大的应用可能会占用所有集群资源，这就导致其它应用被阻塞。在共享集群中，更适合采用Capacity Scheduler或Fair Scheduler，这两个调度器都允许大任务和小任务在提交的同时获得一定的系统资源。

### 5.2.   Capacity Scheduler

Capacity 调度器允许多个组织共享整个集群，每个组织可以获得集群的一部分计算能力。通过为每个组织分配专门的队列，然后再为每个队列分配一定的集群资源，这样整个集群就可以通过设置多个队列的方式给多个组织提供服务了。除此之外，队列内部又可以垂直划分，这样一个组织内部的多个成员就可以共享这个队列资源了，在一个队列内部，资源的调度是采用的是先进先出(FIFO)策略。

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image051.jpg)

容量调度器 Capacity Scheduler 最初是由 Yahoo 最初开发设计使得 Hadoop 应用能够被多用户使用，且最大化整个集群资源的吞吐量，现被 IBM BigInsights 和 Hortonworks HDP 所采用。

Capacity Scheduler 被设计为允许应用程序在一个可预见的和简单的方式共享集群资源，即"作业队列"。Capacity Scheduler 是根据租户的需要和要求把现有的资源分配给运行的应用程序。Capacity Scheduler 同时允许应用程序访问还没有被使用的资源，以确保队列之间共享其它队列被允许的使用资源。管理员可以控制每个队列的容量，Capacity Scheduler 负责把作业提交到队列中。



 

### 5.3.   Fair Scheduler

在Fair调度器中，我们不需要预先占用一定的系统资源，Fair调度器会为所有运行的job动态的调整系统资源。如下图所示，当第一个大job提交时，只有这一个job在运行，此时它获得了所有集群资源；当第二个小任务提交后，Fair调度器会分配一半资源给这个小任务，让这两个任务公平的共享集群资源。

需要注意的是，在下图Fair调度器中，从第二个任务提交到获得资源会有一定的延迟，因为它需要等待第一个任务释放占用的Container。小任务执行完成之后也会释放自己占用的资源，大任务又获得了全部的系统资源。最终效果就是Fair调度器即得到了高的资源利用率又能保证小任务及时完成。

![img](file:////Users/Alfred/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_image053.jpg)

公平调度器 Fair Scheduler 最初是由 Facebook 开发设计使得 Hadoop 应用能够被多用户公平地共享整个集群资源，现被 Cloudera CDH 所采用。

Fair Scheduler 不需要保留集群的资源，因为它会动态在所有正在运行的作业之间平衡资源。



 

### 5.4.   示例：Capacity调度器配置使用

调度器的使用是通过yarn-site.xml配置文件中的

yarn.resourcemanager.scheduler.class参数进行配置的，默认采用Capacity Scheduler调度器。

假设我们有如下层次的队列：

root

├── prod

└── dev

  ├── mapreduce

  └── spark

下面是一个简单的Capacity调度器的配置文件，文件名为capacity-scheduler.xml。在这个配置中，在root队列下面定义了两个子队列prod和dev，分别占40%和60%的容量。需要注意，一个队列的配置是通过属性yarn.sheduler.capacity.<queue-path>.<sub-property>指定的，<queue-path>代表的是队列的继承树，如root.prod队列，<sub-property>一般指capacity和maximum-capacity。

  <configuration>   <property>    <name>**yarn.scheduler.capacity.root.queues**</name>    <value>**prod,dev**</value>   </property>   <property>    <name>**yarn.scheduler.capacity.root.dev.queues**</name>    <value>**mapreduce,spark**</value>   </property>    <property>    <name>**yarn.scheduler.capacity.root.prod.capacity**</name>    <value>**40**</value>   </property>    <property>    <name>**yarn.scheduler.capacity.root.dev.capacity**</name>    <value>**60**</value>   </property>    <property>    <name>**yarn.scheduler.capacity.root.dev.maximum-capacity**</name>    <value>**75**</value>   </property>   <property>    <name>**yarn.scheduler.capacity.root.dev.mapreduce.capacity**</name>    <value>**50**</value>   </property>    <property>    <name>**yarn.scheduler.capacity.root.dev.spark.capacity**</name>    <value>**50**</value>   </property>  </configuration>  

我们可以看到，dev队列又被分成了mapreduce和spark两个相同容量的子队列。dev的maximum-capacity属性被设置成了75%，所以即使prod队列完全空闲dev也不会占用全部集群资源，也就是说，prod队列仍有25%的可用资源用来应急。我们注意到，mapreduce和spark两个队列没有设置maximum-capacity属性，也就是说mapreduce或spark队列中的job可能会用到整个dev队列的所有资源（最多为集群的75%）。而类似的，prod由于没有设置maximum-capacity属性，它有可能会占用集群全部资源。

关于队列的设置，这取决于我们具体的应用。比如，在MapReduce中，我们可以通过`mapreduce.job.queuename`属性指定要用的队列。如果队列不存在，我们在提交任务时就会收到错误。如果我们没有定义任何队列，所有的应用将会放在一个`default`队列中。

注意：对于Capacity调度器，我们的队列名必须是队列树中的最后一部分，如果我们使用队列树则不会被识别。比如，在上面配置中，我们使用`prod`和`mapreduce`作为队列名是可以的，但是如果我们用`root.dev.mapreduce`或者`dev. mapreduce`是无效的。

 

## 6.  .关于yarn常用参数设置

**设置container分配最小内存**

yarn.scheduler.minimum-allocation-mb 1024  给应用程序container分配的最小内存

 

**设置container分配最大内存**

yarn.scheduler.maximum-allocation-mb 8192 给应用程序container分配的最大内存

 

**设置每个container的最小虚拟内核个数**

yarn.scheduler.minimum-allocation-vcores 1 每个container默认给分配的最小的虚拟内核个数

 

**设置每个container的最大虚拟内核个数**

yarn.scheduler.maximum-allocation-vcores 32 每个container可以分配的最大的虚拟内核的个数

 

**设置NodeManager可以分配的内存大小**

yarn.nodemanager.resource.memory-mb 8192 nodemanager 可以分配的最大内存大小，默认8192Mb

 

**定义每台机器的内存使用大小**

yarn.nodemanager.resource.memory-mb 8192

 

**定义交换区空间可以使用的大小**

交换区空间就是讲一块硬盘拿出来做内存使用,这里指定的是nodemanager的2.1倍

yarn.nodemanager.vmem-pmem-ratio 2.1 

 

 

# 三、Hadoop3.x的介绍

**介绍**

  由于Hadoop 2.0是基于JDK 1.7开发的，而JDK 1.7在2015年4月已停止更新，这直接迫使Hadoop社区基于JDK 1.8重新发布一个新的Hadoop版本，即hadoop 3.0。Hadoop 3.0中引入了一些重要的功能和优化，包括HDFS 可擦除编码、多Namenode支持、MR Native Task优化、YARN基于cgroup的内存和磁盘IO隔离、YARN container resizing等。

 

hadoop3.x以后将会调整方案架构，将Mapreduce 基于内存+io+磁盘，共同处理数据。Hadoop3.x改变最大的是hdfs,hdfs 通过最近block块计算，根据最近计算原则，本地block块，加入到内存，先计算，通过IO，共享内存计算区域，最后快速形成计算结果，比Spark快10倍。

 

**Hadoop 3.0****新特性**

Hadoop 3.0在功能和性能方面，对hadoop内核进行了多项重大改进，主要包括：

**通用性**

1.精简Hadoop内核，包括剔除过期的API和实现，将默认组件实现替换成最高效的实现。

\2.  Classpath isolation：以防止不同版本jar包冲突

\3.  Shell脚本重构： Hadoop 3.0对Hadoop的管理脚本进行了重构，修复了大量bug，增加了新特性。

 

**HDFS**

Hadoop3.x中Hdfs在可靠性和支持能力上作出很大改观：

1.HDFS支持数据的擦除编码，这使得HDFS在不降低可靠性的前提下，节省一半存储空间。

2.多NameNode支持，即支持一个集群中，一个active、多个standby namenode部署方式。

**MapReduce**

Hadoop3.X中的MapReduce较之前的版本作出以下更改：

1.Tasknative优化：为MapReduce增加了C/C++的map output collector实现（包括Spill，Sort和IFile等），通过作业级别参数调整就可切换到该实现上。对于shuffle密集型应用，其性能可提高约30%。

2.MapReduce内存参数自动推断。在Hadoop 2.0中，为MapReduce作业设置内存参数非常繁琐，一旦设置不合理，则会使得内存资源浪费严重，在Hadoop3.0中避免了这种情况。

 

**HDFS****纠删码**

在Hadoop3.X中，HDFS实现了Erasure Coding这个新功能。Erasure coding纠删码技术简称EC，是一种数据保护技术.最早用于通信行业中数据传输中的数据恢复，是一种编码容错技术。

 

它通过在原始数据中加入新的校验数据，使得各个部分的数据产生关联性。在一定范围的数据出错情况下，通过纠删码技术都可以进行恢复。

 

hadoop-3.0之前，HDFS存储方式为每一份数据存储3份，这也使得存储利用率仅为1/3，hadoop-3.0引入纠删码技术(EC技术)，实现1份数据+0.5份冗余校验数据存储方式。

 

与副本相比纠删码是一种更节省空间的数据持久化存储方法。标准编码(比如Reed-Solomon(10,4))会有1.4 倍的空间开销；然而HDFS副本则会有3倍的空间开销。

 

**MapReduce****优化** 

Hadoop3.x中的MapReduce添加了Map输出collector的本地实现，对于shuffle密集型的作业来说，这将会有30%以上的性能提升。

 

**支持多个NameNodes** 

   最初的HDFS NameNode high-availability实现仅仅提供了一个active NameNode和一个Standby NameNode；并且通过将编辑日志复制到三个JournalNodes上，这种架构能够容忍系统中的任何一个节点的失败。

  然而，一些部署需要更高的容错度。我们可以通过这个新特性来实现，其允许用户运行多个Standby NameNode。比如通过配置三个NameNode和五个JournalNodes，这个系统可以容忍2个节点的故障，而不是仅仅一个节点。

 

**默认端口更改**

  在hadoop3.x之前，多个Hadoop服务的默认端口都属于Linux的临时端口范围（32768-61000）。这就意味着用户的服务在启动的时候可能因为和其他应用程序产生端口冲突而无法启动。

现在这些可能会产生冲突的端口已经不再属于临时端口的范围，这些端口的改变会影响NameNode, Secondary NameNode, DataNode以及KMS。

Namenode ports: 50470 --> 9871, 50070--> 9870, 8020 --> 9820

Secondary NN ports: 50091 --> 9869,50090 --> 9868

Datanode ports: 50020 --> 9867, 50010--> 9866, 50475 --> 9865, 50075 --> 9864

Kms server ports: 16000 --> 9600 (原先的16000与HMaster端口冲突)

 

**YARN** **资源类型**

  YARN 资源模型（YARN resource model）已被推广为支持用户自定义的可数资源类型（support user-defined countable resource types），不仅仅支持 CPU 和内存。

 

  比如集群管理员可以定义诸如 GPUs、软件许可证（software licenses）或本地附加存储器（locally-attached storage）之类的资源。YARN 任务可以根据这些资源的可用性进行调度。

 

 