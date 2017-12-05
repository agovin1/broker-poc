package test.netty;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.NetUtil;

public class EpollSpliceTest {

	private static final int SPLICE_LEN = 32 * 1024;
	private static final Random random = new Random();
	private static final byte[] data = new byte[1048576];

	static {
		random.nextBytes(data);
	}

	public static void main(String[] args) throws Throwable {
		spliceToSocket();
	}

	private static void spliceToFile() throws Throwable {
		EventLoopGroup group = new EpollEventLoopGroup(1);
		File file = File.createTempFile("netty-splice", null);
		// file.deleteOnExit();

		SpliceHandler sh = new SpliceHandler(file);
		ServerBootstrap bs = new ServerBootstrap();
		bs.channel(EpollServerSocketChannel.class);
		bs.group(group).childHandler(sh);
		bs.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
		Channel sc = bs.bind(NetUtil.LOCALHOST, 0).syncUninterruptibly().channel(); // 0
																					// port
																					// channel
																					// -
																					// server
																					// channel

		Bootstrap cb = new Bootstrap();
		cb.group(group);
		cb.channel(EpollSocketChannel.class);
		cb.handler(new ChannelInboundHandlerAdapter());
		Channel cc = cb.connect(sc.localAddress()).syncUninterruptibly().channel();// connect
																					// to
																					// 0
																					// port-
																					// client
																					// channel

		for (int i = 0; i < data.length;) {
			int length = Math.min(random.nextInt(1024 * 64), data.length - i);
			ByteBuf buf = Unpooled.wrappedBuffer(data, i, length);
			cc.writeAndFlush(buf);
			i += length;
		}

		while (sh.future == null || !sh.future.isDone()) {
			if (sh.exception.get() != null) {
				break;
			}
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// Ignore.
			}
		}

		sc.close().sync();
		cc.close().sync();

		if (sh.exception.get() != null && !(sh.exception.get() instanceof IOException)) {
			throw sh.exception.get();
		}

		byte[] written = new byte[data.length];
		FileInputStream in = new FileInputStream(file);

		try {
			if (data.length != in.read(written)) {
				System.out.println("Not equal");
			}
			if (!Arrays.equals(data, written)) {
				System.out.println("data and written arrays not equal");
			}
			// Assert.assertEquals(written.length, in.read(written));
			// Assert.assertArrayEquals(data, written);
		} finally {
			in.close();
			group.shutdownGracefully();
		}
	}

	private static void spliceToSocket() throws Throwable {

		EchoHandler echoHandler = new EchoHandler();

		EventLoopGroup group = new EpollEventLoopGroup();
		ServerBootstrap sb = new ServerBootstrap();
		sb.channel(EpollServerSocketChannel.class);
		sb.group(group).childHandler(echoHandler);
		final Channel sc = sb.bind(NetUtil.LOCALHOST, 0).syncUninterruptibly().channel();

		ServerBootstrap sb2 = new ServerBootstrap();
		sb2.channel(EpollServerSocketChannel.class);
		sb2.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
		sb2.group(group).childHandler(new ChannelInboundHandlerAdapter() {

			@Override
			public void channelActive(final ChannelHandlerContext ctx) throws Exception {
				ctx.channel().config().setAutoRead(false);

				Bootstrap bs = new Bootstrap();
				bs.option(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
				bs.channel(EpollSocketChannel.class);
				bs.group(ctx.channel().eventLoop()).handler(new ChannelInboundHandlerAdapter() {

					@Override
					public void channelActive(ChannelHandlerContext context) throws Exception {

						final EpollSocketChannel ch = (EpollSocketChannel) ctx.channel();
						final EpollSocketChannel ch2 = (EpollSocketChannel) context.channel();

						ch.spliceTo(ch2, Integer.MAX_VALUE).addListener(new ChannelFutureListener() {

							public void operationComplete(ChannelFuture future) throws Exception {
								if (!future.isSuccess()) {
									future.channel().close();
								}
							}
						});

						ch2.spliceTo(ch, SPLICE_LEN).addListener(new ChannelFutureListener() {

							public void operationComplete(ChannelFuture future) throws Exception {
								if (!future.isSuccess()) {
									future.channel().close();
								} else {
									ch2.spliceTo(ch, SPLICE_LEN).addListener(this);
								}
							}
						});
						ctx.channel().config().setAutoRead(true);
					}
				});

				bs.connect(sc.localAddress()).addListener(new ChannelFutureListener() {
					public void operationComplete(ChannelFuture future) throws Exception {
						if (!future.isSuccess()) {
							ctx.close();
						} else {
							future.channel().closeFuture().addListener(new ChannelFutureListener() {
								public void operationComplete(ChannelFuture future) throws Exception {
									ctx.close();
								}
							});
						}
					}
				});

			}
		});

		Channel pc = sb2.bind(NetUtil.LOCALHOST, 0).syncUninterruptibly().channel();

		EchoHandler echoHandler2 = new EchoHandler();
		Bootstrap cb = new Bootstrap();
		cb.group(group);
		cb.channel(EpollSocketChannel.class);
		cb.handler(echoHandler2);
		Channel cc = cb.connect(pc.localAddress()).syncUninterruptibly().channel();

		for (int i = 0; i < data.length;) {
			int length = Math.min(random.nextInt(1024 * 64), data.length - i);
			ByteBuf buf = Unpooled.wrappedBuffer(data, i, length);
			cc.writeAndFlush(buf);
			i += length;
		}

		while (echoHandler2.counter < data.length) {
			if (echoHandler.exception.get() != null) {
				break;
			}
			if (echoHandler2.exception.get() != null) {
				break;
			}

			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// Ignore.
			}
		}

		while (echoHandler.counter < data.length) {
			if (echoHandler.exception.get() != null) {
				break;
			}
			if (echoHandler2.exception.get() != null) {
				break;
			}

			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// Ignore.
			}

			echoHandler.channel.close().sync();
			echoHandler2.channel.close().sync();
			sc.close().sync();
			pc.close().sync();
			group.shutdownGracefully();

			if (echoHandler.exception.get() != null && !(echoHandler.exception.get() instanceof IOException)) {
				throw echoHandler.exception.get();
			}
			if (echoHandler2.exception.get() != null && !(echoHandler2.exception.get() instanceof IOException)) {
				throw echoHandler2.exception.get();
			}
			if (echoHandler.exception.get() != null) {
				throw echoHandler.exception.get();
			}
			if (echoHandler2.exception.get() != null) {
				throw echoHandler2.exception.get();
			}
		}
	}

	private static class EchoHandler extends SimpleChannelInboundHandler<ByteBuf> {

		volatile Channel channel;
		final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
		volatile int counter;

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			channel = ctx.channel();
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
			byte[] actual = new byte[msg.readableBytes()];
			msg.readBytes(actual);

			int lastIdx = counter;
			for (int i = 0; i < actual.length; i++) {
				assertEquals(data[i + lastIdx], actual[i]);
			}

			if (channel.parent() != null) {
				channel.write(Unpooled.wrappedBuffer(actual));
			}

			counter += actual.length;
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

	private static class SpliceHandler extends ChannelInboundHandlerAdapter {
		private final File file;

		volatile Channel channel;
		volatile ChannelFuture future;
		final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();

		public SpliceHandler(File file) {
			this.file = file;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			channel = ctx.channel();
			final EpollSocketChannel ch = (EpollSocketChannel) ctx.channel();
			final FileDescriptor fd = FileDescriptor.from(file);
			System.out.println("splice call...");
			future = ch.spliceTo(fd, 0, data.length);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			if (exception.compareAndSet(null, cause)) {
				cause.printStackTrace();
				ctx.close();
			}
		}
	}
}
