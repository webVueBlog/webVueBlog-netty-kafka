package example;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpKafkaServerHandler extends SimpleChannelInboundHandler<Object> {// 处理HTTP请求

    private Producer<String, String> kafkaProducer;// Kafka生产者

    public HttpKafkaServerHandler(Producer<String, String> kafkaProducer) {// 构造函数，传入Kafka生产者实例
        this.kafkaProducer = kafkaProducer;// 赋值给成员变量
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {// 读取操作完成
        ctx.flush();// 刷新操作
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, Object msg) {// 处理接收到的消息
        if (msg instanceof HttpRequest) {// 检查消息是否为HTTP请求
            // 处理HTTP请求...
            HttpRequest httpRequest = (HttpRequest) msg;// 强制类型转换

            String uri = httpRequest.getUri();// 获取请求的URI

            KeyedMessage<String, String> message = new KeyedMessage<>("example", uri);// 创建Kafka消息
            kafkaProducer.send(message);// 将消息发送到Kafka

            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);// 构造HTTP响应
            ctx.write(response).addListener(ChannelFutureListener.CLOSE);// 写入响应并关闭连接
        }
    }
}
