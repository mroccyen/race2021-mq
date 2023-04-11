package io.openmessaging.common;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

/**
 * 从其他比赛代码里面考过来的类，就为了用个unsafe
 * @author chender
 * @date 2021/9/12 15:15
 */
public class ResourceManager {
    private static Unsafe unsafe = initUnsafe();
    private static Field capacityField;
    private static Field addressField;

    private static Unsafe initUnsafe() {
        try {
            final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
                public Unsafe run() throws Exception {
                    Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                    theUnsafe.setAccessible(true);
                    Unsafe unsafe = (Unsafe) theUnsafe.get(null);
                    try {
                        capacityField = Buffer.class.getDeclaredField("capacity");
                        capacityField.setAccessible(true);
                        addressField = Buffer.class.getDeclaredField("address");
                        addressField.setAccessible(true);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                    return unsafe;
                }
            };
            return AccessController.doPrivileged(action);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    public static Unsafe getUnsafe() {
        return unsafe;
    }

}
