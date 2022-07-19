/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.storage.safstore.factory;

import org.dspace.app.itemimport.service.ItemImportService;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.dspace.storage.safstore.SAFItemImportServiceImpl;
import org.dspace.storage.safstore.service.SAFItemImportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Factory implementation to get services for the itemimport package, use
 * ItemImportService.getInstance() to retrieve
 * an implementation
 *
 * @author kevinvandevelde at atmire.com
 */
public class SAFItemImportServiceFactory {

    @Autowired(required = true)
    private SAFItemImportService safimport;

    public SAFItemImportService getSafImportService() {
        return safimport;
    }

    public static SAFItemImportServiceFactory getInstance() {
        return DSpaceServicesFactory.getInstance().getServiceManager()
                .getServiceByName(null, SAFItemImportServiceFactory.class);
    }
}
