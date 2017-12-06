package test.netty;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.Date;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;

/**
 * 
 * Message broker component poc using splice with netty. Idea is to minimize
 * user-space copy and compare performance benefits.
 * 
 * @author govind.ajith
 *
 */
public class SpliceBroker {

	public static void main(String[] args) throws Exception {

		final int size = Integer.valueOf(args[0]);
		EventLoopGroup group = new EpollEventLoopGroup(1);
		EventLoopGroup group2 = new EpollEventLoopGroup(1);

		ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.group(group, group2).channel(EpollServerSocketChannel.class);
		serverBootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
		serverBootstrap.childOption(ChannelOption.SO_RCVBUF, size);
		serverBootstrap.childOption(ChannelOption.SO_SNDBUF, size);
		serverBootstrap.childHandler(new ChannelInboundHandlerAdapter() {
			@Override
			public void channelActive(final ChannelHandlerContext ctx) throws Exception {
				ctx.channel().config().setAutoRead(false);
				Bootstrap bootstrap = new Bootstrap();
				bootstrap.channel(EpollSocketChannel.class);
				bootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
				bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(size));
				bootstrap.option(ChannelOption.SO_RCVBUF, size);
				bootstrap.option(ChannelOption.SO_SNDBUF, size);
				bootstrap.group(ctx.channel().eventLoop()).handler(new ChannelInboundHandlerAdapter() {

					@Override
					public void channelActive(final ChannelHandlerContext context) throws Exception {

						final EpollSocketChannel inChannel = (EpollSocketChannel) ctx.channel();
						final EpollSocketChannel outChannel = (EpollSocketChannel) context.channel();

						System.out.println("checking size in bytes - "
								+ context.channel().config().getRecvByteBufAllocator().newHandle().guess());

						inChannel.spliceTo(outChannel, size).addListener(new ChannelFutureListener() {

							public void operationComplete(ChannelFuture future) throws Exception {
								System.out.println("Spliced " + size + " bytes " + LocalDateTime.now().toString());
								if (!future.isSuccess()) {
									future.channel().close();
								} else {
									inChannel.spliceTo(outChannel, size).addListener(this);
								}
							}
						});

						ctx.channel().config().setAutoRead(true);
					}

					@Override
					public void channelInactive(ChannelHandlerContext context) throws Exception {
						context.close();
					}
				});

				bootstrap.connect(InetAddress.getLocalHost(), 31000).addListener(new ChannelFutureListener() {

					public void operationComplete(ChannelFuture future) throws Exception {
						if (!future.isSuccess()) {
							ctx.close();
						} else {
							future.channel().closeFuture().addListener(new ChannelFutureListener() {
								public void operationComplete(ChannelFuture future) throws Exception {
									System.out.println(
											"Closing connection to dest as operation is complete..." + new Date());
									ctx.close();
								}
							});
						}

					}
				});
			}
		});

		Channel pc = serverBootstrap.bind(InetAddress.getLocalHost(), 32000).syncUninterruptibly().channel();
		System.out.println("Sleeping for 10 minutes...");
		Thread.sleep(600000);
		System.out.println("Closing server channel and shutting down the thread eventloopgroup");
		pc.close().sync();
		group.shutdownGracefully();
	}
}
