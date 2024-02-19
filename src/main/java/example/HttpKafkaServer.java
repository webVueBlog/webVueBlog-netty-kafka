package example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class HttpKafkaServer {// 继承自netty的ServerBootstrap

    static final int PORT = 8080;// 定义端口号

    public static void main(String[] args) throws Exception {// 创建两个EventLoopGroup实例
        Properties kafkaProperties = new Properties();// 加载kafka配置文件
        kafkaProperties.load(HttpKafkaServer.class.getResourceAsStream("/kafka.properties"));// 创建Kafka生产者
        ProducerConfig kafkaConfig = new ProducerConfig(kafkaProperties);
        Producer<String, String> kafkaProducer = new Producer<>(kafkaConfig);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);// 作用是接受进来的连接
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();// 绑定两个EventLoopGroup实例到ServerBootstrap
            b.group(bossGroup, workerGroup)// 使用 NioServerSocketChannel 作为服务器通道实现
                    .channel(NioServerSocketChannel.class)// 设置用于处理已接受通道的 ChannelInboundHandler
                    .handler(new LoggingHandler(LogLevel.INFO))// 添加日志处理
                    .childHandler(new HttpKafkaInitializer(kafkaProducer));// 添加我们自己的初始化器
            Channel ch = b.bind(PORT).sync().channel();// 绑定端口并等待服务器绑定完成
            ch.closeFuture().sync();// 等待服务器关闭
        } finally {
            bossGroup.shutdownGracefully();// 关闭EventLoopGroup实例
            workerGroup.shutdownGracefully();// 关闭EventLoopGroup实例
        }
    }

}
