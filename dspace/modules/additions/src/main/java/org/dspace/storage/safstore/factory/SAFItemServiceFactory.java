package org.dspace.storage.safstore.factory;

import org.dspace.services.factory.DSpaceServicesFactory;
import org.dspace.storage.safstore.service.SAFItemRegisterService;
import org.dspace.storage.safstore.service.SAFItemUpdateService;
import org.springframework.beans.factory.annotation.Autowired;

public class SAFItemServiceFactory {

    @Autowired(required = true)
    private SAFItemRegisterService safregister;

    @Autowired(required = true)
    private SAFItemUpdateService safupdate;

    public SAFItemRegisterService getSAFImportService() {
        return safregister;
    }

    public SAFItemUpdateService getSAFUpdateService() {
        return safupdate;
    }

    public static SAFItemImportServiceFactory getInstance() {
        return DSpaceServicesFactory.getInstance().getServiceManager()
                .getServiceByName(null, SAFItemImportServiceFactory.class);
    }

}
