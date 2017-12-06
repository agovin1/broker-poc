package test.netty;

import java.time.LocalDateTime;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;

public class TestBroker {

	public static void main(String[] args) throws Exception {

		final int size = Integer.valueOf(args[0]);
		EventLoopGroup group = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(1);
		System.out.println("Copy broker...");
		ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.group(group, workerGroup).channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						System.out.println("Initializing channel..." + LocalDateTime.now().toString());
						ch.pipeline().addLast(new FixedLengthFrameDecoder(size));
						ch.pipeline().addLast(new TestHandler(size));

					}
				}).option(ChannelOption.SO_RCVBUF, size).option(ChannelOption.SO_BACKLOG, 128)
				.childOption(ChannelOption.SO_KEEPALIVE, true);

		ChannelFuture f = serverBootstrap.bind(32000).sync();

		f.channel().closeFuture().sync();
	}
}
