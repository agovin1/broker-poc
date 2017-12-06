package test.netty;

import java.net.InetAddress;
import java.time.LocalDateTime;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

public class TestHandler extends ChannelInboundHandlerAdapter {

	int size;
	Bootstrap b = new Bootstrap();

	public TestHandler(int size) {
		super();
		this.size = size;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		System.out.println("active channel..." + LocalDateTime.now().toString());
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {

		System.out.println("Reading..." + LocalDateTime.now().toString());

		b.channel(NioSocketChannel.class);
		b.option(ChannelOption.SO_RCVBUF, size);
		b.option(ChannelOption.SO_SNDBUF, size);
		b.group(ctx.channel().eventLoop()).handler(new ChannelInboundHandlerAdapter() {

			@Override
			public void channelActive(final ChannelHandlerContext context) throws Exception {
				System.out.println("Dest connection active.. Starting write " + LocalDateTime.now().toString());
				context.writeAndFlush(msg).addListener(new ChannelFutureListener() {

					public void operationComplete(ChannelFuture future) throws Exception {
						System.out.println("Write complete..." + LocalDateTime.now().toString());
						if (context.channel().isActive()) {
							context.channel().writeAndFlush(Unpooled.EMPTY_BUFFER)
									.addListener(ChannelFutureListener.CLOSE);
						}

					}
				});
			}

			@Override
			public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
				cause.printStackTrace();
				ctx.close();
			}
		});

		b.connect(InetAddress.getLocalHost(), 31000).addListener(new ChannelFutureListener() {

			public void operationComplete(ChannelFuture future) throws Exception {
				if (!future.isSuccess()) {
					ctx.close();
				} else {
					future.channel().closeFuture().addListener(new ChannelFutureListener() {
						public void operationComplete(ChannelFuture future) throws Exception {
							System.out.println("Closing connection to dest as operation is complete..."
									+ LocalDateTime.now().toString());
							ctx.close();
						}
					});
				}

			}
		});
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
		cause.printStackTrace();
		ctx.close();
	}
}
