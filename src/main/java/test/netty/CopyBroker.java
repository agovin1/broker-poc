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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * 
 * Message broker component poc using copy with netty.
 * 
 * @author govind.ajith
 *
 */
public class CopyBroker {

	public static void main(String[] args) throws Exception {

		final int size = Integer.valueOf(args[0]);
		EventLoopGroup group = new NioEventLoopGroup(1);

		ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.group(group).channel(NioServerSocketChannel.class);
		serverBootstrap.childHandler(new ChannelInitializer<NioSocketChannel>() {

			@Override
			protected void initChannel(NioSocketChannel ch) throws Exception {
				ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {

					@Override
					public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {

						System.out.println("Reading from channel..." + LocalDateTime.now().toString());
						Bootstrap bootstrap = new Bootstrap();
						bootstrap.channel(NioSocketChannel.class);
						bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(size));
						bootstrap.group(ctx.channel().eventLoop()).handler(new ChannelInboundHandlerAdapter() {

							@Override
							public void channelActive(final ChannelHandlerContext context) throws Exception {

								System.out.println("Dest channel active. Writing..." + LocalDateTime.now().toString());
								final NioSocketChannel outChannel = (NioSocketChannel) context.channel();
								outChannel.writeAndFlush(msg);
								System.out.println("Write over..." + LocalDateTime.now().toString());
							}
						});

						bootstrap.connect(InetAddress.getLocalHost(), 31000).addListener(new ChannelFutureListener() {

							public void operationComplete(ChannelFuture future) throws Exception {
								if (!future.isSuccess()) {
									ctx.close();
								} else {
									future.channel().closeFuture().addListener(new ChannelFutureListener() {
										public void operationComplete(ChannelFuture future) throws Exception {
											System.out.println("Closing connection to dest as operation is complete..."
													+ new Date());
											ctx.close();
										}
									});
								}

							}
						});
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
