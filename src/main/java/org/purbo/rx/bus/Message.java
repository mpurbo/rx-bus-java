package org.purbo.rx.bus;

import java.util.Map;

/**
 * @author purbo
 */
public interface Message {

    public String id();
    public Map<String, Object> data();
    public Object data(String key);
    public boolean hasData(String key);

}
