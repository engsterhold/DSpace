/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.storage.safstore.factory;

import org.dspace.app.itemexport.service.ItemExportService;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.dspace.storage.safstore.service.SAFItemExportService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Factory implementation to get services for the itemexport package, use
 * ItemExportServiceFactory.getInstance() to
 * retrieve an implementation
 *
 * @author kevinvandevelde at atmire.com
 */
public class SAFItemExportServiceFactory {

    @Autowired(required = true)
    private SAFItemExportService safexport;

    public SAFItemExportService getSAFItemExportService() {
        return safexport;
    }

    public static SAFItemExportServiceFactory getInstance() {
        return DSpaceServicesFactory.getInstance().getServiceManager()
                .getServiceByName(null, SAFItemExportServiceFactory.class);
    }
}
