package org.purbo.rx.bus;

import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.subjects.PublishSubject;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author purbo
 */
public class Bus {

    private static final Logger LOG = Logger.getLogger(Bus.class.getName());

    private static Bus sharedInstance;

    public static String MSGDATAKEY_DEFAULT_DATA_INT = "org.purbo.rx.bus.data.int";
    public static String MSGDATAKEY_DEFAULT_DATA_STRING = "org.purbo.rx.bus.data.string";
    public static String MSGDATAKEY_DEFAULT_DATA_BOOLEAN = "org.purbo.rx.bus.data.boolean";

    /**
     * Subject to propagate all message posts to subscribers.
     */
    PublishSubject<Message> publishSubject;

    /**
     * Map message ID to observable that replays last message with ID as specified by the map key.
     */
    Map<String, Observable<Message>> replayLastObservables;

    public synchronized static Bus getInstance() {
        return sharedInstance == null ? initInstance() : sharedInstance;
    }

    private static Bus initInstance() {
        sharedInstance = new Bus();
        return sharedInstance;
    }

    private Bus() {
        this.publishSubject = PublishSubject.create();
        this.replayLastObservables = new HashMap<String, Observable<Message>>();
    }

    public void post(Message message) {
        LOG.finest(
                "Posting message with ID: " + message.id() +
                        ", data: " + message.data() != null ? message.data().toString() : "null");
        publishSubject.onNext(message);
    }

    public void post(final String id, final Map<String, Object> data) {
        post(new Message() {
            @Override
            public String id() {
                return id;
            }

            @Override
            public Map<String, Object> data() {
                return data;
            }

            @Override
            public Object data(String key) {
                if (data != null) {
                    return data.get(key);
                }
                return null;
            }

            @Override
            public boolean hasData(String key) {
                if (data != null) {
                    return data.get(key) != null;
                }
                return false;
            }
        });
    }

    public void post(final String id, final int data) {
        HashMap<String, Object> wrapper = new HashMap<String, Object>();
        wrapper.put(MSGDATAKEY_DEFAULT_DATA_INT, data);
        post(id, wrapper);
    }

    public void post(final String id, final String data) {
        HashMap<String, Object> wrapper = new HashMap<String, Object>();
        wrapper.put(MSGDATAKEY_DEFAULT_DATA_STRING, data);
        post(id, wrapper);
    }

    public void post(final String id, final boolean data) {
        HashMap<String, Object> wrapper = new HashMap<String, Object>();
        wrapper.put(MSGDATAKEY_DEFAULT_DATA_BOOLEAN, data);
        post(id, wrapper);
    }

    /**
     *
     * @return Observable for observing all messages posted to the bus.
     */
    public Observable<Message> messages() {
        return publishSubject;
    }

    private <T> Observable<T> messagesWithDefaultData(Observable<Message> messages, final String defaultDataKey) {
        return messages
                .filter(new Predicate<Message>() {
                    @Override
                    public boolean test(Message message) throws Exception {
                        return message.hasData(defaultDataKey);
                    }
                })
                .map(new Function<Message, T>() {
                    @Override
                    public T apply(Message message) throws Exception {
                        return (T)message.data(defaultDataKey);
                    }
                });
    };

    private Predicate<Message> filterMessagePredicate(final String id) {
        return new Predicate<Message>() {
            @Override
            public boolean test(Message message) throws Exception {
                return message.id().equals(id);
            }
        };
    }

    /**
     *
     * @param id Message ID to be observed.
     * @return Observable for observing messages with the specified ID that was posted to the bus.
     */
    public Observable<Message> messages(final String id) {
        return publishSubject.filter(filterMessagePredicate(id));
    }

    public Observable<Integer> integerMessages(final String id) {
        return messagesWithDefaultData(messages(id), MSGDATAKEY_DEFAULT_DATA_INT);
    }

    public Observable<String> stringMessages(final String id) {
        return messagesWithDefaultData(messages(id), MSGDATAKEY_DEFAULT_DATA_STRING);
    }

    public Observable<Boolean> booleanMessages(final String id) {
        return messagesWithDefaultData(messages(id), MSGDATAKEY_DEFAULT_DATA_BOOLEAN);
    }

    public synchronized boolean isMessagesReplayLastObservableCreated(final String id) {
        return replayLastObservables.get(id) != null;
    }

    public synchronized Observable<Message> messagesReplayLast(final String id) {
        Observable<Message> ret = replayLastObservables.get(id);
        if (ret == null) {
            LOG.finest("Creating replay last observable for id: " + id);
            ret = messages(id).replay(1).autoConnect();
            replayLastObservables.put(id, ret);
        }
        return ret;
    }

    public Observable<Integer> integerMessagesReplayLast(final String id) {
        return messagesWithDefaultData(messagesReplayLast(id), MSGDATAKEY_DEFAULT_DATA_INT);
    }

    public Observable<String> stringMessagesReplayLast(final String id) {
        return messagesWithDefaultData(messagesReplayLast(id), MSGDATAKEY_DEFAULT_DATA_STRING);
    }

    public Observable<Boolean> booleanMessagesReplayLast(final String id) {
        return messagesWithDefaultData(messagesReplayLast(id), MSGDATAKEY_DEFAULT_DATA_BOOLEAN);
    }

    public synchronized Single<Message> lastMessage(final String id) {
        return messagesReplayLast(id).firstOrError();
    }

    public Single<Integer> integerLastMessage(final String id) {
        return integerMessagesReplayLast(id).firstOrError();
    }

    public Single<String> stringLastMessage(final String id) {
        return stringMessagesReplayLast(id).firstOrError();
    }

    public Single<Boolean> booleanLastMessage(final String id) {
        return booleanMessagesReplayLast(id).firstOrError();
    }

}
