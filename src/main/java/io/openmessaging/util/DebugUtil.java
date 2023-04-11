package io.openmessaging.util;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author chender
 * @date 2021/5/19 13:46
 */
public class DebugUtil {

    private static OperatingSystemMXBean osMxBean = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();

    public static void printMemory(String append) {
        System.out.println(Thread.currentThread().toString() + "@" + new SimpleDateFormat("HH:mm:ss").format(new Date()) + ",cpu:" + osMxBean.getAvailableProcessors() + ","  + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + "/" + Runtime.getRuntime().freeMemory() + "/" + Runtime.getRuntime().maxMemory() + "," + append);
    }
}
