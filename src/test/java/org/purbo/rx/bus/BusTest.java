package org.purbo.rx.bus;

import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.functions.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author purbo
 */
public class BusTest {

    CompositeDisposable disposables;
    Bus bus;

    static final String TEST_MESSAGE_ID = "abcd";

    static final String [] TEST_DATA_KEYS = new String [] {"test1", "test2", "test3"};
    static final Integer [] TEST_DATA_VALUES = new Integer [] {1234, 5678, 9012};

    static final String TEST_STRING_MESSAGE_ID = "abcdString";
    static final String TEST_STRING_VALUE = "testStringValue";

    static final String TEST_REPLAY_MESSAGE_ID = "abcdReplay";

    @Before
    public void setUp() throws Exception {
        disposables = new CompositeDisposable();
        bus = Bus.getInstance();
    }

    @After
    public void tearDown() throws Exception {
        disposables.dispose();
    }

    @Test
    public void testMessages() throws Exception {
        // see: http://jodah.net/testing-multi-threaded-code

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<Exception>();

        disposables.add(
                Bus.getInstance()
                        .messages(TEST_MESSAGE_ID)
                        .subscribe(
                                new Consumer<Message>() {
                                    @Override
                                    public void accept(Message message) throws Exception {
                                        assertEquals(message.id(), TEST_MESSAGE_ID);
                                        assertTrue(message.hasData(TEST_DATA_KEYS[0]));
                                        assertEquals(message.data(TEST_DATA_KEYS[0]), TEST_DATA_VALUES[0]);
                                        latch.countDown();
                                    }
                                },
                                new Consumer<Throwable>() {
                                    @Override
                                    public void accept(Throwable throwable) throws Exception {
                                        failure.set(new Exception(throwable));
                                        latch.countDown();
                                    }
                                }
                        ));

        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put(TEST_DATA_KEYS[0], TEST_DATA_VALUES[0]);
        Bus.getInstance().post(TEST_MESSAGE_ID, data);

        latch.await();
        if (failure.get() != null)
            throw failure.get();
    }

    @Test
    public void testStringMessages() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<Exception>();

        disposables.add(
                Bus.getInstance()
                        .stringMessages(TEST_STRING_MESSAGE_ID)
                        .subscribe(
                                new Consumer<String>() {
                                    @Override
                                    public void accept(String string) throws Exception {
                                        assertEquals(string, TEST_STRING_VALUE);
                                        latch.countDown();
                                    }
                                },
                                new Consumer<Throwable>() {
                                    @Override
                                    public void accept(Throwable throwable) throws Exception {
                                        failure.set(new Exception(throwable));
                                        latch.countDown();
                                    }
                                }
                        ));

        Bus.getInstance().post(TEST_STRING_MESSAGE_ID, TEST_STRING_VALUE);

        latch.await();
        if (failure.get() != null)
            throw failure.get();
    }

}