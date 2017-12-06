package test.netty;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.nio.NioSocketChannel;

public class CopyHandler extends SimpleChannelInboundHandler<ByteBuf> {

	volatile Channel channel;
	final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
	volatile int counter;
	volatile Bootstrap bootstrap;
	NioSocketChannel outChannel;
	int size;
	byte[] actual;
	Channel dest;

	public CopyHandler(int size) {
		super();
		this.size = size;
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		System.out.println("Request connection active.." + LocalDateTime.now().toString());
		channel = ctx.channel();
		bootstrap = new Bootstrap();
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.SO_RCVBUF, size);
		bootstrap.option(ChannelOption.SO_SNDBUF, size);

		bootstrap.group(ctx.channel().eventLoop()).handler(new ChannelInboundHandlerAdapter() {

			@Override
			public void channelActive(final ChannelHandlerContext context) throws Exception {
				dest = context.channel();
			}

		});

		bootstrap.connect(InetAddress.getLocalHost(), 31000).addListener(new ChannelFutureListener() {

			public void operationComplete(ChannelFuture future) throws Exception {
				if (!future.isSuccess()) {
					ctx.close();
				} else {
					System.out.println("Dest connected.." + LocalDateTime.now().toString());
				}

			}
		});
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		// ChannelConfig config = ctx.channel().config();
		// System.out.println(config.getOptions());
		actual = new byte[in.readableBytes()];
		in.readBytes(actual);
		System.out.println("Length of bytes copied is " + actual.length + " " + actual.toString());
		System.out.println(dest.remoteAddress());
		dest.writeAndFlush(actual).addListener(new ChannelFutureListener() {

			public void operationComplete(ChannelFuture future) throws Exception {
				System.out.println("Write is complete. Closing both ends.." + LocalDateTime.now().toString());
				dest.close();
				ctx.close();
			}
		});
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (exception.compareAndSet(null, cause)) {
			cause.printStackTrace();
			ctx.close();
		}
	}

}
