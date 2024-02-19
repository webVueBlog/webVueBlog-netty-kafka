package example;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import kafka.javaapi.producer.Producer;

public class HttpKafkaInitializer extends ChannelInitializer<SocketChannel> {// 继承ChannelInitializer类

    private Producer<String, String> kafkaProducer;// 声明一个Kafka生产者对象

    public HttpKafkaInitializer(Producer<String, String> kafkaProducer) {// 构造函数，传入Kafka生产者对象
        this.kafkaProducer = kafkaProducer;// 赋值给成员变量
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {// 初始化通道
        ChannelPipeline p = ch.pipeline();// 获取通道的管道
        p.addLast(new HttpRequestDecoder());// 添加HTTP请求解码器
        p.addLast(new HttpResponseEncoder());// 添加HTTP响应编码器
        p.addLast(new HttpKafkaServerHandler(kafkaProducer));// 添加自定义的HTTP处理器
    }
}
