package com.teach.business;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.functions.col;

import java.util.*;


public class ReadNewsStream {
    private static final Logger logger = LoggerFactory.getLogger(ReadNewsStream.class);
    public static final Properties serverProps = PropertiesUtils.getProperties("config.properties");
    static Broadcast<Properties> serverPropsBroadcast=null;


    public static void main(String[] args) throws Exception {
        /* 获取 checkpoint 的 hdfs 路径 */
        String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");
        /* 如果 checkpointPath hdfs 目录下的有文件，则反序列化文件生产 context, 否则使用函数 createContext 返回的 context 对象 */
        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointPath, createContext(serverProps));
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    /**
     * 根据配置文件以及业务逻辑创建 JavaStreamingContext
     *
     * @param serverProps
     * @return
     */

    public static Function0<JavaStreamingContext> createContext(final Properties serverProps) {
        Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
            public JavaStreamingContext call() throws Exception {
                //获取配置中的 topic
                logger.info("配置是" + serverProps.toString());
                String topicStr = serverProps.getProperty("kafka.topic");
                Collection<String> topics = Arrays.asList(topicStr.split(","));
                //获取配置中的 groupId
                final String groupId = serverProps.getProperty("kafka.groupId");
                //获取批次的时间间隔，比如 5s
                final Long streamingInterval = Long.parseLong(serverProps.getProperty("streaming.interval"));
                //获取 checkpoint 的 hdfs 路径
                final String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");
                //获取 kafka broker 列表
                final String kafkaBrokerList = serverProps.getProperty("kafka.broker.list");
                //组合 kafka 参数
                final Map<String, Object> kafkaParams = new HashMap();
                kafkaParams.put("bootstrap.servers", kafkaBrokerList);
                kafkaParams.put("group.id", groupId);
                kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                // 创建 SparkConf 对象
                SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-news");

                /**
                 优雅停止 Spark. 暴力停掉 sparkstreaming 是有可能出现问题的，比如你的数据源是 kafka，
                 已经加载了一批数据到 sparkstreaming 中正在处理，如果中途停掉，
                 这个批次的数据很有可能没有处理完，就被强制 stop 了，
                 下次启动时候会重复消费或者部分数据丢失。
                 */
                sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");

                /*在 Spark 的架构中，在网络中传递的或者缓存在内存、硬盘中的对象需要进行序列化操作，序列化的作用主要是利用时间换空间*/
                sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

                /*增加 MyRegistrator 类，注册需要用 Kryo 序列化的类,Kryo 的序列化会比java的要更省空间*/
                 sparkConf.set("spark.kryo.registrator", "com.teach.business.MyKryoRegistor");

                /*    每秒钟对于每个 partition 读取spark.streaming.backpressure.enabled多少条数据。如果不进行设置，Spark Streaming 会一开始就读取 partition 中的所有数据到内存，给内存造成巨大压力
                 设置此参数后可以很好地控制 Spark Streaming 读取的数据量，也可以说控制了读取的进度 */
                sparkConf.set("spark.streaming.kafka.maxRatePerPartition", serverProps.getProperty("streaming.kafka.maxRatePerPartition"));

                /*    创建 javaStreamingContext，设置 每隔5s 执行一次*/
                JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(streamingInterval));

                serverPropsBroadcast=javaStreamingContext.sparkContext().broadcast(serverProps);

               /* check point 的存在，每次都会从中读取元数据和RDD，则每次的代码修改是无效。因为kafka的offset存在，所以checkPoint可以不用。仅做参考 */
//                javaStreamingContext.checkpoint(checkpointPath);

                //创建 kafka DStream
                final JavaInputDStream<ConsumerRecord<String, String>> kafkaMessage = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));


                /*  spark sql 会话 */
                logger.info("spark conf 是{}",javaStreamingContext.sparkContext().sc().conf());
                SparkSession sparkSession = SparkSession.builder().enableHiveSupport().sparkContext(javaStreamingContext.sparkContext().sc()).config(sparkConf).getOrCreate();

                /* 启动的时候 先删除 停止路径 */
                Configuration conf = javaStreamingContext.sparkContext().sc().hadoopConfiguration();
                FileSystem fs = DistributedFileSystem.get(conf);
                Path stopPath=new org.apache.hadoop.fs.Path(serverProps.getProperty("streaming.stop.path"));
                if(fs.exists(stopPath)){
                    logger.info("删除历史中的stop path");
                    fs.delete(stopPath,true);
                }

                //  需要把每个批次的 offset 保存
                kafkaMessage.foreachRDD(rdd -> {
                    logger.info("开始迭代");

                    /*  如果stop path出现,就停止系统 */
                    if(fs.exists(stopPath)){
                        logger.info("开始停止spark程序");
                        javaStreamingContext.stop(true);
                    }

                    /*  表示具有[[OffsetRange]]集合的任何对象，这可以用来访问由直 Direct Kafka DStream 生成的 RDD 中的偏移量范围*/
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    /*  逻辑处理在这里*/
                    executeData(rdd, sparkSession, serverProps);
                    /*  kafka offset 写入 zk*/
                    ((CanCommitOffsets) kafkaMessage.inputDStream()).commitAsync(offsetRanges);
                });
                //  将 kafka 中的消息转换成对象并过滤不合法的消息
                return javaStreamingContext;
            }
        };
        return createContextFunc;
    }

    /**
     * 处理业务逻辑
     * @param rdd
     * @param sparkSession
     * @param serverProps
     */
    static private void executeData(JavaRDD<ConsumerRecord<String, String>> rdd, SparkSession sparkSession, Properties serverProps) {
        //1.计算威尔逊热度
        JavaRDD<Row> rowRDD = rdd.filter(stringStringConsumerRecord -> {
            /*过滤出来只有点击的日志*/
            String content = stringStringConsumerRecord.value();
            return content.contains("display") && content.contains("|");
        }).flatMap(stringStringConsumerRecord -> {
            /*组装成新闻的点击格式*/
            String val = stringStringConsumerRecord.value();
            ObjectMapper objectMapper = new ObjectMapper();
            Long timestam = Long.parseLong(val.split("\\|")[0]);
            JsonNode jsonNode = objectMapper.readTree(val.split("\\|")[1]);
            //      用户id，news id，area，时间
            String userid = jsonNode.get("cm").get("uid").asText();
            String area = jsonNode.get("cm").get("ar").asText();
            Iterator<JsonNode> iterater = jsonNode.get("et").iterator();
            List<Row> results = new ArrayList<>();
            while (iterater.hasNext()) {
                JsonNode event = iterater.next();
                if ("display".equals(event.get("en").asText())) {
                    int action = event.get("kv").get("action").asInt();
                    String newsId = event.get("kv").get("newsid").asText();
                    Row row = RowFactory.create(action, area, newsId, userid, timestam);
                    results.add(row);
                }
            }
            return results.iterator();
        });
        ArrayList<StructField> fields = new ArrayList<>();
        StructField field = null;
        field = DataTypes.createStructField("action", DataTypes.IntegerType, true);
        fields.add(field);
        field = DataTypes.createStructField("area", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("news_id", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("user_id", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("time_stam", DataTypes.LongType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);

        if (!rowRDD.isEmpty()) {
            Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);
            /* 先对写入的日志去重，减少写入RDS的数据 */
            df.registerTempTable("tmp_input_display");
            df=sparkSession.sql("select action,area,news_id,user_id,time_stam from tmp_input_display group by action,area,news_id,user_id,time_stam");
            /*   写入rds,根据24小时的数据去重 */
            Properties prop = new java.util.Properties();
            prop.setProperty("driver", "com.mysql.jdbc.Driver");
            prop.setProperty("user", serverProps.getProperty("mysql.user"));
            prop.setProperty("password", serverProps.getProperty("mysql.password"));
            df.write().mode(SaveMode.Overwrite).jdbc(serverProps.getProperty("mysql.url"), "tmp_display_click", prop);
            /*  去重点击和展示两个表 */
            MysqlService.execUpdate("delete t1 from news_display t1 join (select area,news_id,user_id,time_stam from tmp_display_click where action=1) t2 " +
                    "on t1.area=t2.area and t1.news_id=t2.news_id and t1.user_id=t2.user_id", null);
            MysqlService.execUpdate("insert ignore into news_display select area,news_id,user_id,time_stam from tmp_display_click where action=1", null);
            MysqlService.execUpdate("delete t1 from news_display t1 join (select area,news_id,user_id,time_stam from tmp_display_click where action=2) t2 \n" +
                    "on t1.area=t2.area and t1.news_id=t2.news_id and t1.user_id=t2.user_id", null);
            MysqlService.execUpdate("insert ignore into news_click select area,news_id,user_id,time_stam from tmp_display_click where action=2", null);
            /*   获取新闻的点击率  24小时内的数据比较少，可以直接在mysql里面join了。*/
            df=sparkSession.read().jdbc(serverProps.getProperty("mysql.url"), "(select t1.*," +
                    "case when t2.click is not null then t2.click else 0 end click  from \n" +
                    "(select count(distinct user_id) display,news_id,area from news_display group by area,news_id) t1 " +
                    "left join " +
                    "(select count(distinct user_id) click ,news_id,area from news_click group by area,news_id) t2 " +
                    "on t1.news_id = t2.news_id) click_info", prop);
//            df.show();
            df.registerTempTable("click_info");
            /* 删除超过24小时的数据，此处因为测试，所以没写了。*/
            /* 读取24小时内的新闻质量 */
            long afterTime=System.currentTimeMillis()/1000-36*24*60*60;
            Dataset<Row> qualityDf=sparkSession.read().jdbc(serverProps.getProperty("mysql.url"), "(select news_id,quality from news_quality where time_stam>"+afterTime+") tmp_news_quality", prop);
            qualityDf.registerTempTable("tmp_news_quality");
            sparkSession.sqlContext().udf().register("cal_score",(UDF3<Long,Long,Double,Double>)( click, display, quality)-> {
                /* 根据点击率，质量，计算新闻的分数,此处是调用算法的函数，所以写了一个随机数 */
                    return new Random().nextDouble();
            },DataTypes.DoubleType);

            /* 点击和质量数据进行join，然后算出打分，最后排序 */
            df=sparkSession.sql("select t1.news_id,t1.area,t1.display,t1.click,t2.quality,cal_score(t1.click,t1.display,t2.quality) score from click_info t1 join tmp_news_quality t2 " +
                    "on t1.news_id=t2.news_id");
            df.show();
            df.registerTempTable("tmp_news_score");

            //计算结果写入kafka，另一端消费kafka写入数据库保存记录
            df.foreachPartition((ForeachPartitionFunction<Row>) (iterator)->{
                Properties props = new Properties();
                props.put("bootstrap.servers", serverPropsBroadcast.getValue().getProperty("kafka.broker.list"));
                props.put("acks", "all");
                props.put("retries", 0);
                props.put("batch.size", 16384);
                props.put("linger.ms", 1);
                props.put("buffer.memory", 33554432);
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                KafkaProducer<String, String> scoreProducer = new KafkaProducer<>(props);

                while (iterator.hasNext()){
                    Row row=iterator.next();
                    String json=String.format("{\"news_id\":\"%s\",\"score\":%s}",row.getString(0),row.getDouble(5));
                    ProducerRecord<String, String> msg = new ProducerRecord<>("news_score_test",json);
                    scoreProducer.send(msg);
                }
                scoreProducer.flush();
            });
//       最后将新闻排序,取前1000条，写入mysql，传给后台
            df=sparkSession.sql("select news_id from tmp_news_score order by score limit 1000");
            String rankNews=StringUtils.join(df.collectAsList(),",");
            logger.info("测试结果是"+rankNews);
        }
    }
}
