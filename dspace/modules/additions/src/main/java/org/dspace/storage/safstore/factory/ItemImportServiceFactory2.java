/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.storage.safstore.factory;

import org.dspace.app.itemimport.factory.ItemImportServiceFactory;
import org.dspace.app.itemimport.service.ItemImportService;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.dspace.storage.safstore.ItemImportServiceImpl2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Factory implementation to get services for the itemimport package, use
 * ItemImportService.getInstance() to retrieve
 * an implementation
 *
 * @author kevinvandevelde at atmire.com
 */
public class ItemImportServiceFactory2 extends ItemImportServiceFactory {

    @Autowired(required = true)
    @Qualifier("itemImportService2")
    private ItemImportServiceImpl2 itemImportService2;

    @Override
    public ItemImportService getItemImportService() {
        return itemImportService2;
    }

    public static ItemImportServiceFactory2 getInstance() {
        return DSpaceServicesFactory.getInstance().getServiceManager()
                .getServiceByName(null, ItemImportServiceFactory2.class);
    }
}
