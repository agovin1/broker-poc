package test.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;

public class CopyChannelInitializer extends ChannelInitializer<NioSocketChannel> {

	int size;

	public CopyChannelInitializer(int size) {
		super();
		this.size = size;
	}

	@Override
	protected void initChannel(NioSocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
		pipeline.addLast(new FixedLengthFrameDecoder(size));
		pipeline.addLast(new CopyHandler(size));
	}

}
