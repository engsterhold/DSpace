/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.storage.safstore;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.dspace.content.Bundle;
import org.dspace.content.DSpaceObject;
import org.dspace.content.MetadataValue;
import org.dspace.content.service.BitstreamService;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.discovery.indexobject.factory.IndexFactory;
import org.dspace.discovery.indexobject.factory.IndexObjectFactoryFactory;
import org.dspace.event.Consumer;
import org.dspace.event.Event;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.dspace.discovery.IndexableObject;
import org.dspace.discovery.IndexingService;
import org.dspace.content.Item;
import org.dspace.content.Bitstream;

/**
 * Class for updating search indices in discovery from content events.
 *
 * @author Kevin Van de Velde (kevin at atmire dot com)
 * @author Mark Diggory (markd at atmire dot com)
 * @author Ben Bosman (ben at atmire dot com)
 */
public class SAFBitStoreEventConsumer implements Consumer {
    /**
     * log4j logger
     */
    private static Logger log = org.apache.logging.log4j.LogManager.getLogger(SAFBitStoreEventConsumer.class);

    // collect Items, Collections, Communities that need indexing
    private Set<DSpaceObject> objectsToUpdate = new HashSet<>();

    // unique search IDs to delete
    private Set<String> uniqueIdsToDelete = new HashSet<>();

    @Override
    public void initialize() throws Exception {

    }

    /**
     * Consume a content event -- just build the sets of objects to add (new) to
     * the index, update, and delete.
     *
     * @param ctx   DSpace context
     * @param event Content event
     */
    @Override
    public void consume(Context ctx, Event event) throws Exception {

        log.info("I am the SAF BitStore Consumer, ctx: " + ctx.toString() + " Event: " + event.toString());
        if (objectsToUpdate == null) {
            objectsToUpdate = new HashSet<>();
            uniqueIdsToDelete = new HashSet<>();
        }

        int st = event.getSubjectType();
        if (!(st == Constants.ITEM || st == Constants.BUNDLE
                || st == Constants.BITSTREAM)) {
            log
                    .warn("SAFBitStoreEventConsumer should not have been given this kind of Subject in an event, skipping: "
                            + event.toString());
            return;
        }

        DSpaceObject subject = event.getSubject(ctx);

        DSpaceObject object = event.getObject(ctx);

        // If event subject is a Bundle and event was Add or Remove,
        // transform the event to be a Modify on the owning Item.
        // It could be a new bitstream in the TEXT bundle which
        // would change the index.
        int et = event.getEventType();
        if (st == Constants.BUNDLE) {
            if ((et == Event.ADD || et == Event.REMOVE) && subject != null
                    && ((Bundle) subject).getName().equals("TEXT")) {
                st = Constants.ITEM;
                et = Event.MODIFY;
                subject = ((Bundle) subject).getItems().get(0);
                if (log.isDebugEnabled()) {
                    log.debug("Transforming Bundle event into MODIFY of Item "
                            + subject.getHandle());
                }
            } else {
                return;
            }
        }

        switch (et) {
            case Event.CREATE:
            case Event.MODIFY:
            case Event.MODIFY_METADATA:
                if (subject == null) {

                } else {
                    log.debug("consume() adding event to update queue: " + event.toString());

                    objectsToUpdate.add(subject);
                }
                break;

            case Event.REMOVE:
            case Event.ADD:
                if (object == null) {
                    log.warn(event.getEventTypeAsString() + " event, could not get object for "
                            + event.getObjectTypeAsString() + " id="
                            + event.getObjectID()
                            + ", perhaps it has been deleted.");
                } else {
                    log.debug("consume() adding event to update queue: " + event.toString());
                    objectsToUpdate.add(subject);
                }
                break;

            case Event.DELETE:
                if (event.getSubjectType() == -1 || event.getSubjectID() == null) {
                    log.warn("got null subject type and/or ID on DELETE event, skipping it.");
                } else {

                    log.debug("consume() adding event to delete queue: " + event.toString());
                    String detail = subject.getID().toString();
                    log.debug("consume() Subject details: " + subject.getID().toString());
                    uniqueIdsToDelete.add(detail);
                }
                break;
            default:
                log
                        .warn("SAFBitStoreEventConsumer should not have been given a event of type="
                                + event.getEventTypeAsString()
                                + " on subject="
                                + event.getSubjectTypeAsString());
                break;
        }
    }

    /**
     * Process sets of objects to add, update, and delete in index. Correct for
     * interactions between the sets -- e.g. objects which were deleted do not
     * need to be added or updated, new objects don't also need an update, etc.
     */
    @Override
    public void end(Context ctx) throws Exception {
        log.info("This is the end, my only friend the..." + ctx.toString());
        log.info("OBJECTS TO UPDATE: " + objectsToUpdate.toString());

        try {
            for (String uid : uniqueIdsToDelete) {
                try {

                    if (log.isDebugEnabled()) {
                        log.debug("delete Item, handle=" + uid);
                    }

                } catch (Exception e) {
                    log.error("Failed while UN-indexing object: " + uid, e);
                }
            }
            // update the changed Items not deleted because they were on create list
            for (DSpaceObject iu : objectsToUpdate) {
                /*
                 * we let all types through here and
                 * allow the search indexer to make
                 * decisions on indexing and/or removal
                 */

                Item x = null;
                if (iu instanceof Item) {
                    x = (Item) iu;

                    List<Bitstream> b = x.getBundles().stream()
                            .flatMap(s -> s.getBitstreams().stream())
                            .collect(Collectors.toList());
                    log.debug("First=" + b.get(0).getName() + " Bitstream_id=" + b.get(0).getInternalId());

                }

                String uniqueIndexID = iu.getID().toString();
                if (uniqueIndexID != null) {
                    try {

                        log.debug("Does something with Name="
                                + iu.getName()
                                + ", id=" + iu.getID() + ", Metadata="
                                + Arrays.toString(iu.getMetadata().stream().map(s -> s.getValue()).toArray()));
                    } catch (Exception e) {
                        log.error("Failed while indexing object: ", e);
                    }
                }
            }
        } finally {
            if (!objectsToUpdate.isEmpty() || !uniqueIdsToDelete.isEmpty()) {

                log.debug("stuff not empty, commit here?");

                // "free" the resources
                objectsToUpdate.clear();
                uniqueIdsToDelete.clear();
            }
        }
    }

    @Override
    public void finish(Context ctx) throws Exception {
        // No-op

    }

}
