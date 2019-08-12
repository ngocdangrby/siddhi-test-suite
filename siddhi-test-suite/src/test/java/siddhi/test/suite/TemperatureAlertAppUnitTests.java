package siddhi.test.suite;

import com.google.common.io.Resources;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class TemperatureAlertAppUnitTests {
    private static final Logger logger = LoggerFactory.getLogger(TemperatureAlertAppUnitTests.class);
    private static URL appUrl = Resources.getResource("TemperatureAlertApp/apps/Temp-Alert-App_UnitTesting.siddhi");
    private volatile AtomicInteger count = new AtomicInteger(0);

    @Test
    public void testMoniteredFilter() throws InterruptedException, IOException {
        logger.info("Tests Monitered Filter Query");

        String testQueryName = "monitered-filter";
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = readFileToString(appUrl.getPath());

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback(testQueryName, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                count.incrementAndGet();
                if (count.get() == 1) {
                    Assert.assertTrue("CyrusOne".equals(inEvents[0].getData(0)));
                }
                if (count.get() == 2) {
                    Assert.assertTrue("Kennisnet".equals(inEvents[0].getData(0)));
                }
            }
        });
        InputHandler deviceTemperatureStream = siddhiAppRuntime.getInputHandler("DeviceTemperatureStream");
        siddhiAppRuntime.start();

        deviceTemperatureStream.send(new Object[]{"monitored", "CyrusOne", 35.5, "ServerRoom1"});
        deviceTemperatureStream.send(new Object[]{"internal", "Generator", 28.6, "Basement"});
        deviceTemperatureStream.send(new Object[]{"monitored", "Kennisnet", 60.2, "ServerRoom5"});
        SiddhiTestHelper.waitForEvents(10, 2, count, 100);
        siddhiAppRuntime.shutdown();
    }

    private String readFileToString (String filePath) throws IOException {
        return new String (Files.readAllBytes(Paths.get(filePath)), Charset.forName("UTF-8"));
    }
}
