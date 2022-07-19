package org.dspace.storage.safstore.service;

import java.util.Iterator;
import org.dspace.content.Item;
import org.dspace.core.Context;

public interface SAFItemUpdateService {

    public void updateItem(Context c, Iterator<Item> i) throws Exception;

}
