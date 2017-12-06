package test.netty;

import java.net.InetAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

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
		EventLoopGroup workerGroup = new NioEventLoopGroup(1);

		ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.group(group, workerGroup).channel(NioServerSocketChannel.class);
		serverBootstrap.childHandler(new CopyChannelInitializer(size));
		serverBootstrap.childOption(ChannelOption.SO_RCVBUF, size);
		serverBootstrap.childOption(ChannelOption.SO_SNDBUF, size);

		Channel pc = serverBootstrap.bind(InetAddress.getLocalHost(), 32000).syncUninterruptibly().channel();
		System.out.println("Sleeping for 10 minutes...");
		Thread.sleep(600000);
		System.out.println("Closing server channel and shutting down the thread eventloopgroup");
		pc.close().sync();
		group.shutdownGracefully();
		workerGroup.shutdownGracefully();
	}
}
