package test.netty;

import java.util.UUID;

public class Test1 {

	public static void main(String[] args) {
		UUID uuid = UUID.fromString("1902d921-ed7d-4e0d-9d00-9982adb0d496");
		System.out.println(uuid.getLeastSignificantBits() + " " + uuid.getMostSignificantBits());
	}
}
