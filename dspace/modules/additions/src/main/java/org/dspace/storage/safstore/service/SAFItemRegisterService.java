package org.dspace.storage.safstore.service;

import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.mail.MessagingException;

import org.dspace.app.itemimport.BatchUpload;
import org.dspace.content.Collection;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;

public interface SAFItemRegisterService {

    public void registerItems(Context c, List<Collection> mycollections,
            String sourceDir, String mapFile, boolean template) throws Exception;

    public void unregisterItems(Context c, String mapFile) throws Exception;

    public void updateItems(Context c, List<Collection> mycollections,
            String sourceDir, String mapFile, boolean template) throws Exception;

    public void setTest(boolean isTest);

    public void setResume(boolean isResume);

    public void setUseWorkflow(boolean useWorkflow);

    public void setUseWorkflowSendEmail(boolean useWorkflowSendEmail);

    public void setQuiet(boolean isQuiet);
}
