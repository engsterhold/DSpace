/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.storage.safstore;

import static org.dspace.iiif.util.IIIFSharedUtils.METADATA_IIIF_HEIGHT_QUALIFIER;
import static org.dspace.iiif.util.IIIFSharedUtils.METADATA_IIIF_IMAGE_ELEMENT;
import static org.dspace.iiif.util.IIIFSharedUtils.METADATA_IIIF_LABEL_ELEMENT;
import static org.dspace.iiif.util.IIIFSharedUtils.METADATA_IIIF_SCHEMA;
import static org.dspace.iiif.util.IIIFSharedUtils.METADATA_IIIF_TOC_ELEMENT;
import static org.dspace.iiif.util.IIIFSharedUtils.METADATA_IIIF_WIDTH_QUALIFIER;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.mail.MessagingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.collections4.ComparatorUtils;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;
import org.dspace.app.itemimport.service.ItemImportService;
import org.dspace.app.util.LocalSchemaFilenameFilter;
import org.dspace.app.util.RelationshipUtils;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.authorize.service.AuthorizeService;
import org.dspace.authorize.service.ResourcePolicyService;
import org.dspace.content.Bitstream;
import org.dspace.content.BitstreamFormat;
import org.dspace.content.Bundle;
import org.dspace.content.Collection;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.MetadataField;
import org.dspace.content.MetadataSchema;
import org.dspace.content.MetadataSchemaEnum;
import org.dspace.content.MetadataValue;
import org.dspace.content.Relationship;
import org.dspace.content.RelationshipType;
import org.dspace.content.WorkspaceItem;
import org.dspace.content.service.BitstreamFormatService;
import org.dspace.content.service.BitstreamService;
import org.dspace.content.service.BundleService;
import org.dspace.content.service.CollectionService;
import org.dspace.content.service.InstallItemService;
import org.dspace.content.service.ItemService;
import org.dspace.content.service.MetadataFieldService;
import org.dspace.content.service.MetadataSchemaService;
import org.dspace.content.service.MetadataValueService;
import org.dspace.content.service.RelationshipService;
import org.dspace.content.service.RelationshipTypeService;
import org.dspace.content.service.WorkspaceItemService;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.core.Email;
import org.dspace.core.I18nUtil;
import org.dspace.core.LogHelper;
import org.dspace.eperson.EPerson;
import org.dspace.eperson.Group;
import org.dspace.eperson.service.EPersonService;
import org.dspace.eperson.service.GroupService;
import org.dspace.handle.service.HandleService;
import org.dspace.services.ConfigurationService;
import org.dspace.storage.safstore.service.SAFItemImportService;
import org.dspace.storage.safstore.service.SAFItemRegisterService;
import org.dspace.storage.bitstore.factory.StorageServiceFactory;
import org.dspace.storage.bitstore.service.BitstreamStorageService;
import org.dspace.workflow.WorkflowItem;
import org.dspace.workflow.WorkflowService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.dspace.app.itemimport.*;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Import items into DSpace. The conventional use is upload files by copying
 * them. DSpace writes the item's bitstreams into its assetstore. Metadata is
 * also loaded to the DSpace database.
 * <P>
 * A second use assumes the bitstream files already exist in a storage
 * resource accessible to DSpace. In this case the bitstreams are 'registered'.
 * That is, the metadata is loaded to the DSpace database and DSpace is given
 * the location of the file which is subsumed into DSpace.
 * <P>
 * The distinction is controlled by the format of lines in the 'contents' file.
 * See comments in processContentsFile() below.
 * <P>
 * Modified by David Little, UCSD Libraries 12/21/04 to
 * allow the registration of files (bitstreams) into DSpace.
 */
public class SAFItemRegisterServiceImpl implements SAFItemRegisterService, InitializingBean {
    private final Logger log = org.apache.logging.log4j.LogManager.getLogger(SAFItemImportServiceImpl.class);

    @Autowired(required = true)
    protected AuthorizeService authorizeService;
    @Autowired(required = true)
    protected BitstreamService bitstreamService;
    @Autowired(required = true)
    protected BitstreamFormatService bitstreamFormatService;
    @Autowired(required = true)
    protected BundleService bundleService;
    @Autowired(required = true)
    protected CollectionService collectionService;
    @Autowired(required = true)
    protected EPersonService ePersonService;
    @Autowired(required = true)
    protected HandleService handleService;
    @Autowired(required = true)
    protected ItemService itemService;
    @Autowired(required = true)
    protected InstallItemService installItemService;
    @Autowired(required = true)
    protected GroupService groupService;
    @Autowired(required = true)
    protected MetadataFieldService metadataFieldService;
    @Autowired(required = true)
    protected MetadataSchemaService metadataSchemaService;
    @Autowired(required = true)
    protected ResourcePolicyService resourcePolicyService;
    @Autowired(required = true)
    protected WorkspaceItemService workspaceItemService;
    @Autowired(required = true)
    protected WorkflowService workflowService;
    @Autowired(required = true)
    protected ConfigurationService configurationService;
    @Autowired(required = true)
    protected RelationshipService relationshipService;
    @Autowired(required = true)
    protected RelationshipTypeService relationshipTypeService;
    @Autowired(required = true)
    protected MetadataValueService metadataValueService;

    protected String tempWorkDir;
    protected String safStoreDir;

    protected boolean isTest = false;
    protected boolean isResume = false;
    protected boolean useWorkflow = false;
    protected boolean useWorkflowSendEmail = false;
    protected boolean isQuiet = false;
    protected int importMode = -1;
    protected String[] safItems = { "dublin_core.xml", "contents" };

    // remember which folder item was imported from
    Map<String, Item> itemFolderMap = null;
    BitstreamStorageService bitStores = StorageServiceFactory.getInstance().getBitstreamStorageService();

    @Override
    public void afterPropertiesSet() throws Exception {
        tempWorkDir = configurationService.getProperty("org.dspace.app.batchitemimport.work.dir");
        // Ensure tempWorkDir exists
        File tempWorkDirFile = new File(tempWorkDir);
        if (!tempWorkDirFile.exists()) {
            boolean success = tempWorkDirFile.mkdir();
            if (success) {
                log.info("Created org.dspace.app.batchitemimport.work.dir of: " + tempWorkDir);
            } else {
                log.error("Cannot create batch import directory! " + tempWorkDir);
            }
        }
        safStoreDir = configurationService.getProperty("safstore.dir");
        File safStoreDirFile = new File(safStoreDir);
        if (!(safStoreDirFile.exists() && safStoreDirFile.isDirectory())) {
            throw new FileNotFoundException(
                    "safStoreDir.dir is not set or does not point to a folder. safStoreDir: " + safStoreDir);
        }

    }

    // File listing filter to look for metadata files
    protected FilenameFilter metadataFileFilter = new LocalSchemaFilenameFilter();

    // File listing filter to check for folders
    protected FilenameFilter directoryFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String n) {
            File item = new File(dir.getAbsolutePath() + File.separatorChar + n);
            return item.isDirectory();
        }
    };

    protected SAFItemRegisterServiceImpl() {
        // Protected consumer to ensure that we use spring to create a bean, NEVER make
        // this public

    }

    // chekc if the import item is part of the saf store
    // TODO: More sanitizing
    protected boolean checkFilePath(String sourceDir) {

        return Paths.get(sourceDir).startsWith(safStoreDir);

    }

    // If the source path is three layers deep it is in single item mode if it is
    // two it is in collection mode, everything else should raise an error
    protected int checkImportMode(String sourceDir) {
        Path relativePath = Paths.get(safStoreDir).relativize(Paths.get(sourceDir));
        List<Path> list = new ArrayList<Path>();
        relativePath.iterator().forEachRemaining(list::add);
        return list.size();
    }

    @Override
    public void registerItems(Context c, List<Collection> mycollections,
            String sourceDir, String mapFile, boolean template) throws Exception {

        if (!checkFilePath(sourceDir)) {
            throw new AccessDeniedException("source dir is not child of the saf store. SourceDir" + sourceDir
                    + " safStore: " + safStoreDir);
        }
        importMode = checkImportMode(sourceDir);
        System.out.print("Depth of Item: " + importMode);

        // create the mapfile
        File outFile = null;
        PrintWriter mapOut = null;
        System.out.println("SAF Store folder" + configurationService.getProperty("safstore.dir"));
        System.out.println("Incoming store" + configurationService.getProperty("assetstore.incoming"));
        try {

            Map<String, String> skipItems = new HashMap<>(); // set of items to skip if in 'resume'
            // mode

            itemFolderMap = new HashMap<>();

            System.out.println("Adding items from directory: " + sourceDir);
            log.debug("Adding items from directory: " + sourceDir);
            System.out.println("Generating mapfile: " + mapFile);
            log.debug("Generating mapfile: " + mapFile);

            boolean directoryFileCollections = false;
            if (mycollections == null) {
                directoryFileCollections = true;
            }

            if (!isTest) {
                // get the directory names of items to skip (will be in keys of
                // hash)
                if (isResume) {
                    skipItems = readMapFile(mapFile);
                }

                // sneaky isResume == true means open file in append mode
                outFile = new File(mapFile);
                mapOut = new PrintWriter(new FileWriter(outFile, isResume));

                if (mapOut == null) {
                    throw new Exception("can't open mapfile: " + mapFile);
                }
            }

            // open and process the source directory
            File d = new java.io.File(sourceDir);

            System.out.println("File d: " + d.getName());
            log.debug("File d: " + d.getName());

            if (d == null || !d.isDirectory()) {
                throw new Exception("Error, cannot open source directory " + sourceDir);
            }

            String[] dircontents;

            if (importMode == 3 && Arrays.asList(d.list()).containsAll(Arrays.asList(safItems))) {
                System.out.println(Arrays.toString(d.list()) + " contains: dublin_core.xml");
                log.debug(Arrays.toString(d.list()) + " contains: dublin_core.xml");
                dircontents = new String[] { "." };

            } else if (importMode == 2) {

                System.out.println(Arrays.toString(d.list()) + " contains no: dublin_core.xml");
                log.debug(Arrays.toString(d.list()) + " contains no: dublin_core.xml");
                dircontents = d.list(directoryFilter);

            } else {
                throw new AccessDeniedException("importMode needs to be 2 or 3 is: " + importMode);
            }

            Arrays.sort(dircontents, ComparatorUtils.naturalComparator());

            for (int i = 0; i < dircontents.length; i++) {
                if (skipItems.containsKey(dircontents[i])) {
                    System.out.println("Skipping import of " + dircontents[i]);

                    // we still need the item in the map for relationship linking
                    String skippedHandle = skipItems.get(dircontents[i]);
                    Item skippedItem = (Item) handleService.resolveToObject(c, skippedHandle);
                    itemFolderMap.put(dircontents[i], skippedItem);

                } else {
                    List<Collection> clist;
                    if (directoryFileCollections) {
                        String path = sourceDir + File.separatorChar + dircontents[i];
                        try {
                            List<Collection> cols = processCollectionFile(c, path, "collections");
                            if (cols == null) {
                                System.out
                                        .println("No collections specified for item " + dircontents[i] + ". Skipping.");
                                continue;
                            }
                            clist = cols;
                        } catch (IllegalArgumentException e) {
                            System.out.println(e.getMessage() + " Skipping.");
                            continue;
                        }
                    } else {
                        clist = mycollections;
                    }

                    Item item = addItem(c, clist, sourceDir, dircontents[i], mapOut, template);

                    itemFolderMap.put(dircontents[i], item);

                    c.uncacheEntity(item);
                    System.out.println(i + " " + dircontents[i]);
                }
            }

            // now that all items are imported, iterate again to link relationships
            addRelationships(c, sourceDir);

        } finally {
            if (mapOut != null) {
                mapOut.flush();
                mapOut.close();
            }
        }
    }

    /**
     * Add relationships from a 'relationships' manifest file.
     * 
     * @param c         Context
     * @param sourceDir The parent import source directory
     * @throws Exception
     */
    protected void addRelationships(Context c, String sourceDir) throws Exception {

        for (Map.Entry<String, Item> itemEntry : itemFolderMap.entrySet()) {

            String folderName = itemEntry.getKey();
            String path = sourceDir + File.separatorChar + folderName;
            Item item = itemEntry.getValue();

            // look for a 'relationship' manifest
            Map<String, List<String>> relationships = processRelationshipFile(path, "relationships");
            if (!relationships.isEmpty()) {

                for (Map.Entry<String, List<String>> relEntry : relationships.entrySet()) {

                    String relationshipType = relEntry.getKey();
                    List<String> identifierList = relEntry.getValue();

                    for (String itemIdentifier : identifierList) {

                        if (isTest) {
                            System.out.println("\tAdding relationship (type: " + relationshipType +
                                    ") from " + folderName + " to " + itemIdentifier);
                            continue;
                        }

                        // find referenced item
                        Item relationItem = resolveRelatedItem(c, itemIdentifier);
                        if (null == relationItem) {
                            throw new Exception("Could not find item for " + itemIdentifier);
                        }

                        // get entity type of entity and item
                        String itemEntityType = getEntityType(item);
                        String relatedEntityType = getEntityType(relationItem);

                        // find matching relationship type
                        List<RelationshipType> relTypes = relationshipTypeService.findByLeftwardOrRightwardTypeName(
                                c, relationshipType);
                        RelationshipType foundRelationshipType = RelationshipUtils.matchRelationshipType(
                                relTypes, relatedEntityType, itemEntityType, relationshipType);

                        if (foundRelationshipType == null) {
                            throw new Exception("No Relationship type found for:\n" +
                                    "Target type: " + relatedEntityType + "\n" +
                                    "Origin referer type: " + itemEntityType + "\n" +
                                    "with typeName: " + relationshipType);
                        }

                        boolean left = false;
                        if (foundRelationshipType.getLeftwardType().equalsIgnoreCase(relationshipType)) {
                            left = true;
                        }

                        // Placeholder items for relation placing
                        Item leftItem = null;
                        Item rightItem = null;
                        if (left) {
                            leftItem = item;
                            rightItem = relationItem;
                        } else {
                            leftItem = relationItem;
                            rightItem = item;
                        }

                        // Create the relationship
                        int leftPlace = relationshipService.findNextLeftPlaceByLeftItem(c, leftItem);
                        int rightPlace = relationshipService.findNextRightPlaceByRightItem(c, rightItem);
                        Relationship persistedRelationship = relationshipService.create(
                                c, leftItem, rightItem, foundRelationshipType, leftPlace, rightPlace);
                        // relationshipService.update(c, persistedRelationship);

                        System.out.println("\tAdded relationship (type: " + relationshipType + ") from " +
                                leftItem.getHandle() + " to " + rightItem.getHandle());

                    }

                }

            }

        }

    }

    /**
     * Get the item's entity type from meta.
     * 
     * @param item
     * @return
     */
    protected String getEntityType(Item item) throws Exception {
        return itemService.getMetadata(item, "dspace", "entity", "type", Item.ANY).get(0).getValue();
    }

    /**
     * Read the relationship manifest file.
     * 
     * Each line in the file contains a relationship type id and an item identifier
     * in the following format:
     * 
     * relation.<relation_key>
     * <handle|uuid|folderName:import_item_folder|schema.element[.qualifier]:value>
     * 
     * The input_item_folder should refer the folder name of another item in this
     * import batch.
     * 
     * @param path     The main import folder path.
     * @param filename The name of the manifest file to check ('relationships')
     * @return Map of found relationships
     * @throws Exception
     */
    protected Map<String, List<String>> processRelationshipFile(String path, String filename) throws Exception {

        File file = new File(path + File.separatorChar + filename);
        Map<String, List<String>> result = new HashMap<>();

        if (file.exists()) {

            System.out.println("\tProcessing relationships file: " + filename);

            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if ("".equals(line)) {
                        continue;
                    }

                    String relationshipType = null;
                    String itemIdentifier = null;

                    StringTokenizer st = new StringTokenizer(line);

                    if (st.hasMoreTokens()) {
                        relationshipType = st.nextToken();
                        if (relationshipType.split("\\.").length > 1) {
                            relationshipType = relationshipType.split("\\.")[1];
                        }
                    } else {
                        throw new Exception("Bad mapfile line:\n" + line);
                    }

                    if (st.hasMoreTokens()) {
                        itemIdentifier = st.nextToken("").trim();
                    } else {
                        throw new Exception("Bad mapfile line:\n" + line);
                    }

                    if (!result.containsKey(relationshipType)) {
                        result.put(relationshipType, new ArrayList<>());
                    }

                    result.get(relationshipType).add(itemIdentifier);

                }

            } catch (FileNotFoundException e) {
                System.out.println("\tNo relationships file found.");
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        System.out.println("Non-critical problem releasing resources.");
                    }
                }
            }

        }

        return result;
    }

    /**
     * Resolve an item identifier referred to in the relationships manifest file.
     *
     * The import item map will be checked first to see if the identifier refers to
     * an item folder
     * that was just imported. Next it will try to find the item by handle or UUID,
     * or by a unique
     * meta value.
     * 
     * @param c              Context
     * @param itemIdentifier The identifier string found in the import manifest
     *                       (handle, uuid, or import subfolder)
     * @return Item if found, or null.
     * @throws Exception
     */
    protected Item resolveRelatedItem(Context c, String itemIdentifier) throws Exception {

        if (itemIdentifier.contains(":")) {

            if (itemIdentifier.startsWith("folderName:") || itemIdentifier.startsWith("rowName:")) {
                // identifier refers to a folder name in this import
                int i = itemIdentifier.indexOf(":");
                String folderName = itemIdentifier.substring(i + 1);
                if (itemFolderMap.containsKey(folderName)) {
                    return itemFolderMap.get(folderName);
                }

            } else {

                // lookup by meta value
                int i = itemIdentifier.indexOf(":");
                String metaKey = itemIdentifier.substring(0, i);
                String metaValue = itemIdentifier.substring(i + 1);
                return findItemByMetaValue(c, metaKey, metaValue);

            }

        } else if (itemIdentifier.indexOf('/') != -1) {
            // resolve by handle
            return (Item) handleService.resolveToObject(c, itemIdentifier);

        } else {
            // try to resolve by UUID
            return itemService.findByIdOrLegacyId(c, itemIdentifier);
        }

        return null;

    }

    /**
     * Lookup an item by a (unique) meta value.
     * 
     * @param metaKey
     * @param metaValue
     * @return Item
     * @throws Exception if single item not found.
     */
    protected Item findItemByMetaValue(Context c, String metaKey, String metaValue) throws Exception {

        Item item = null;

        String mf[] = metaKey.split("\\.");
        if (mf.length < 2) {
            throw new Exception("Bad metadata field in reference: '" + metaKey +
                    "' (expected syntax is schema.element[.qualifier])");
        }
        String schema = mf[0];
        String element = mf[1];
        String qualifier = mf.length == 2 ? null : mf[2];
        try {
            MetadataField mfo = metadataFieldService.findByElement(c, schema, element, qualifier);
            Iterator<MetadataValue> mdv = metadataValueService.findByFieldAndValue(c, mfo, metaValue);
            if (mdv.hasNext()) {
                MetadataValue mdvVal = mdv.next();
                UUID uuid = mdvVal.getDSpaceObject().getID();
                if (mdv.hasNext()) {
                    throw new Exception("Ambiguous reference; multiple matches in db: " + metaKey);
                }
                item = itemService.find(c, uuid);
            }
        } catch (SQLException e) {
            throw new Exception("Error looking up item by metadata reference: " + metaKey, e);
        }

        if (item == null) {
            throw new Exception("Item not found by metadata reference: " + metaKey);
        }

        return item;

    }

    @Override
    public void updateItems(Context c, List<Collection> mycollections,
            String sourceDir, String mapFile, boolean template) throws Exception {
        // verify the source directory
        File d = new java.io.File(sourceDir);

        if (d == null || !d.isDirectory()) {
            throw new Exception("Error, cannot open source directory "
                    + sourceDir);
        }

        // read in HashMap first, to get list of handles & source dirs
        Map<String, String> myHash = readMapFile(mapFile);

        // for each handle, re-import the item, discard the new handle
        // and re-assign the old handle
        for (Map.Entry<String, String> mapEntry : myHash.entrySet()) {
            // get the old handle
            String newItemName = mapEntry.getKey();
            String oldHandle = mapEntry.getValue();

            Item oldItem = null;

            if (oldHandle.indexOf('/') != -1) {
                System.out.println("\tReplacing:  " + oldHandle);

                // add new item, locate old one
                oldItem = (Item) handleService.resolveToObject(c, oldHandle);
            } else {
                oldItem = itemService.findByIdOrLegacyId(c, oldHandle);
            }

            /*
             * Rather than exposing public item methods to change handles --
             * two handles can't exist at the same time due to key constraints
             * so would require temp handle being stored, old being copied to new and
             * new being copied to old, all a bit messy -- a handle file is written to
             * the import directory containing the old handle, the existing item is
             * deleted and then the import runs as though it were loading an item which
             * had already been assigned a handle (so a new handle is not even assigned).
             * As a commit does not occur until after a successful add, it is safe to
             * do a delete as any error results in an aborted transaction without harming
             * the original item
             */
            File handleFile = new File(sourceDir + File.separatorChar + newItemName + File.separatorChar + "handle");
            PrintWriter handleOut = new PrintWriter(new FileWriter(handleFile, true));

            if (handleOut == null) {
                throw new Exception("can't open handle file: " + handleFile.getCanonicalPath());
            }

            handleOut.println(oldHandle);
            handleOut.close();

            deleteItem(c, oldItem);
            Item newItem = addItem(c, mycollections, sourceDir, newItemName, null, template);
            c.uncacheEntity(oldItem);
            c.uncacheEntity(newItem);
        }
    }

    @Override
    public void unregisterItems(Context c, String mapFile) throws Exception {
        System.out.println("Deleting items listed in mapfile: " + mapFile);

        // read in the mapfile
        Map<String, String> myhash = readMapFile(mapFile);

        // now delete everything that appeared in the mapFile
        Iterator<String> i = myhash.keySet().iterator();

        while (i.hasNext()) {
            String itemID = myhash.get(i.next());

            if (itemID.indexOf('/') != -1) {
                String myhandle = itemID;
                System.out.println("Deleting item " + myhandle);
                deleteItem(c, myhandle);
            } else {
                // it's an ID
                Item myitem = itemService.findByIdOrLegacyId(c, itemID);
                System.out.println("Deleting item " + itemID);
                deleteItem(c, myitem);
                c.uncacheEntity(myitem);
            }
        }
    }

    /**
     * item? try and add it to the archive.
     *
     * @param c             current Context
     * @param mycollections - add item to these Collections.
     * @param path          - directory containing the item directories.
     * @param itemname      handle - non-null means we have a pre-defined handle
     *                      already
     * @param mapOut        - mapfile we're writing
     * @param template      whether to use collection template item as starting
     *                      point
     * @return Item
     * @throws Exception if error occurs
     */
    protected Item addItem(Context c, List<Collection> mycollections, String path,
            String itemname, PrintWriter mapOut, boolean template) throws Exception {
        String mapOutputString = null;

        System.out.println("Adding item from directory " + itemname);
        log.debug("adding item from directory " + itemname);

        // create workspace item
        Item myitem = null;
        WorkspaceItem wi = null;
        WorkflowItem wfi = null;

        if (!isTest) {
            wi = workspaceItemService.create(c, mycollections.iterator().next(), template);
            myitem = wi.getItem();
        }

        // now fill out dublin core for item
        loadMetadata(c, myitem, path + File.separatorChar + itemname
                + File.separatorChar);

        // and the bitstreams from the contents file
        // process contents file, add bistreams and bundles, return any
        // non-standard permissions
        List<String> options = processContentsFile(c, myitem, path
                + File.separatorChar + itemname, "contents");

        if (useWorkflow) {
            // don't process handle file
            // start up a workflow
            if (!isTest) {
                // Should we send a workflow alert email or not?
                if (useWorkflowSendEmail) {
                    wfi = workflowService.start(c, wi);
                } else {
                    wfi = workflowService.startWithoutNotify(c, wi);
                }

                // send ID to the mapfile
                mapOutputString = itemname + " " + myitem.getID();
            }
        } else {
            // only process handle file if not using workflow system
            String myhandle = processHandleFile(c, myitem, path
                    + File.separatorChar + itemname, "handle");

            // put item in system
            if (!isTest) {
                try {
                    installItemService.installItem(c, wi, myhandle);
                } catch (Exception e) {
                    workspaceItemService.deleteAll(c, wi);
                    log.error("Exception after install item, try to revert...", e);
                    throw e;
                }

                // find the handle, and output to map file
                myhandle = handleService.findHandle(c, myitem);

                mapOutputString = itemname + " " + myhandle;
            }

            // set permissions if specified in contents file
            if (options.size() > 0) {
                System.out.println("Processing options");
                processOptions(c, myitem, options);
            }
        }

        // now add to multiple collections if requested
        if (mycollections.size() > 1) {
            for (int i = 1; i < mycollections.size(); i++) {
                if (!isTest) {
                    collectionService.addItem(c, mycollections.get(i), myitem);
                }
            }
        }

        // made it this far, everything is fine, commit transaction
        if (mapOut != null) {
            mapOut.println(mapOutputString);
        }

        // Clear intermediary objects from the cache
        c.uncacheEntity(wi);
        c.uncacheEntity(wfi);

        return myitem;
    }

    // remove, given the actual item
    protected void deleteItem(Context c, Item myitem) throws Exception {
        if (!isTest) {
            ArrayList<Collection> removeList = new ArrayList<>();
            List<Collection> collections = myitem.getCollections();

            // Save items to be removed to prevent concurrent modification exception DS-3322
            for (Collection collection : collections) {
                removeList.add(collection);
            }

            // Remove item from all the collections it's in
            for (Collection collection : removeList) {
                collectionService.removeItem(c, collection, myitem);
            }
        }
    }

    // remove, given a handle
    protected void deleteItem(Context c, String myhandle) throws Exception {
        // bit of a hack - to remove an item, you must remove it
        // from all collections it's a part of, then it will be removed
        Item myitem = (Item) handleService.resolveToObject(c, myhandle);

        if (myitem == null) {
            System.out.println("Error - cannot locate item - already deleted?");
        } else {
            deleteItem(c, myitem);
            c.uncacheEntity(myitem);
        }
    }

    ////////////////////////////////////
    // utility methods
    ////////////////////////////////////
    // read in the map file and generate a hashmap of (file,handle) pairs
    protected Map<String, String> readMapFile(String filename) throws Exception {
        Map<String, String> myHash = new HashMap<>();

        BufferedReader is = null;
        try {
            is = new BufferedReader(new FileReader(filename));

            String line;

            while ((line = is.readLine()) != null) {
                String myFile;
                String myHandle;

                // a line should be archive filename<whitespace>handle
                StringTokenizer st = new StringTokenizer(line);

                if (st.hasMoreTokens()) {
                    myFile = st.nextToken();
                } else {
                    throw new Exception("Bad mapfile line:\n" + line);
                }

                if (st.hasMoreTokens()) {
                    myHandle = st.nextToken();
                } else {
                    throw new Exception("Bad mapfile line:\n" + line);
                }

                myHash.put(myFile, myHandle);
            }
        } finally {
            if (is != null) {
                is.close();
            }
        }

        return myHash;
    }

    // Load all metadata schemas into the item.
    protected void loadMetadata(Context c, Item myitem, String path)
            throws SQLException, IOException, ParserConfigurationException,
            SAXException, TransformerException, AuthorizeException, XPathExpressionException {
        // Load the dublin core metadata
        loadDublinCore(c, myitem, path + "dublin_core.xml");

        // Load any additional metadata schemas
        File folder = new File(path);
        File file[] = folder.listFiles(metadataFileFilter);
        for (int i = 0; i < file.length; i++) {
            loadDublinCore(c, myitem, file[i].getAbsolutePath());
        }
    }

    protected void loadDublinCore(Context c, Item myitem, String filename)
            throws SQLException, IOException, ParserConfigurationException,
            SAXException, TransformerException, AuthorizeException, XPathExpressionException {
        Document document = loadXML(filename);

        // Get the schema, for backward compatibility we will default to the
        // dublin core schema if the schema name is not available in the import
        // file
        String schema;
        XPath xPath = XPathFactory.newInstance().newXPath();
        NodeList metadata = (NodeList) xPath.compile("/dublin_core").evaluate(document, XPathConstants.NODESET);
        Node schemaAttr = metadata.item(0).getAttributes().getNamedItem(
                "schema");
        if (schemaAttr == null) {
            schema = MetadataSchemaEnum.DC.getName();
        } else {
            schema = schemaAttr.getNodeValue();
        }

        // Get the nodes corresponding to formats
        NodeList dcNodes = (NodeList) xPath.compile("/dublin_core/dcvalue").evaluate(document, XPathConstants.NODESET);

        if (!isQuiet) {
            System.out.println("\tLoading dublin core from " + filename);
        }

        // Add each one as a new format to the registry
        for (int i = 0; i < dcNodes.getLength(); i++) {
            Node n = dcNodes.item(i);
            addDCValue(c, myitem, schema, n);
        }
    }

    protected void addDCValue(Context c, Item i, String schema, Node n)
            throws TransformerException, SQLException, AuthorizeException {
        String value = getStringValue(n); // n.getNodeValue();
        // compensate for empty value getting read as "null", which won't display
        if (value == null) {
            value = "";
        } else {
            value = value.trim();
        }
        // //getElementData(n, "element");
        String element = getAttributeValue(n, "element");
        String qualifier = getAttributeValue(n, "qualifier"); // NodeValue();
        // //getElementData(n,
        // "qualifier");
        String language = getAttributeValue(n, "language");
        if (language != null) {
            language = language.trim();
        }

        if (!isQuiet) {
            System.out.println("\tSchema: " + schema + " Element: " + element + " Qualifier: " + qualifier
                    + " Value: " + value);
        }

        if ("none".equals(qualifier) || "".equals(qualifier)) {
            qualifier = null;
        }
        // only add metadata if it is no test and there is an actual value
        if (!isTest && !value.equals("")) {
            itemService.addMetadata(c, i, schema, element, qualifier, language, value);
        } else {
            // If we're just test the import, let's check that the actual metadata field
            // exists.
            MetadataSchema foundSchema = metadataSchemaService.find(c, schema);

            if (foundSchema == null) {
                System.out.println("ERROR: schema '" + schema + "' was not found in the registry.");
                return;
            }

            MetadataField foundField = metadataFieldService.findByElement(c, foundSchema, element, qualifier);

            if (foundField == null) {
                System.out.println(
                        "ERROR: Metadata field: '" + schema + "." + element + "." + qualifier
                                + "' was not found in the " +
                                "registry.");
                return;
            }
        }
    }

    /**
     * Read the collections file inside the item directory. If there
     * is one and it is not empty return a list of collections in
     * which the item should be inserted. If it does not exist or it
     * is empty return null.
     *
     * @param c        The context
     * @param path     The path to the data directory for this item
     * @param filename The collections file filename. Should be "collections"
     * @return A list of collections in which to insert the item or null
     * @throws IOException  if IO error
     * @throws SQLException if database error
     */

    protected List<Collection> processCollectionFile(Context c, String path, String filename)
            throws IOException, SQLException {
        File file = new File(path + File.separatorChar + filename);
        ArrayList<Collection> collections = new ArrayList<>();
        List<Collection> result = null;
        System.out.println("Processing collections file: " + filename);

        if (file.exists()) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    DSpaceObject obj = null;
                    if (line.indexOf('/') != -1) {
                        obj = handleService.resolveToObject(c, line);
                        if (obj == null || obj.getType() != Constants.COLLECTION) {
                            obj = null;
                        }
                    } else {
                        obj = collectionService.find(c, UUID.fromString(line));
                    }

                    if (obj == null) {
                        throw new IllegalArgumentException("Cannot resolve " + line + " to a collection.");
                    }
                    collections.add((Collection) obj);

                }

                result = collections;
            } catch (FileNotFoundException e) {
                System.out.println("No collections file found.");
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        System.out.println("Non-critical problem releasing resources.");
                    }
                }
            }
        }

        return result;
    }

    /**
     * Read in the handle file contents or return null if empty or doesn't exist
     *
     * @param c        DSpace context
     * @param i        DSpace item
     * @param path     path to handle file
     * @param filename name of file
     * @return handle file contents or null if doesn't exist
     */
    protected String processHandleFile(Context c, Item i, String path, String filename) {
        File file = new File(path + File.separatorChar + filename);
        String result = null;

        System.out.println("Processing handle file: " + filename);
        if (file.exists()) {
            BufferedReader is = null;
            try {
                is = new BufferedReader(new FileReader(file));

                // result gets contents of file, or null
                result = is.readLine();

                System.out.println("read handle: '" + result + "'");

            } catch (FileNotFoundException e) {
                // probably no handle file, just return null
                System.out.println("It appears there is no handle file -- generating one");
            } catch (IOException e) {
                // probably no handle file, just return null
                System.out.println("It appears there is no handle file -- generating one");
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e1) {
                        System.err.println("Non-critical problem releasing resources.");
                    }
                }
            }
        } else {
            // probably no handle file, just return null
            System.out.println("It appears there is no handle file -- generating one");
        }

        return result;
    }

    /**
     * Given a contents file and an item, stuffing it with bitstreams from the
     * contents file Returns a List of Strings with lines from the contents
     * file that request non-default bitstream permission
     *
     * @param c        DSpace Context
     * @param i        DSpace item
     * @param path     path as string
     * @param filename file name
     * @return List of Strings
     * @throws SQLException       if database error
     * @throws IOException        if IO error
     * @throws AuthorizeException if authorization error
     */
    protected List<String> processContentsFile(Context c, Item i, String path,
            String filename) throws SQLException, IOException,
            AuthorizeException {
        File contentsFile = new File(path + File.separatorChar + filename);
        String line = "";
        List<String> options = new ArrayList<>();

        System.out.println("\tProcessing contents file: " + contentsFile);

        if (contentsFile.exists()) {
            BufferedReader is = null;
            try {
                is = new BufferedReader(new FileReader(contentsFile));

                while ((line = is.readLine()) != null) {
                    if ("".equals(line.trim())) {
                        continue;
                    }

                    // 1) registered into dspace (leading -r)
                    // 2) imported conventionally into dspace (no -r)
                    if (line.trim().startsWith("-r ")) {
                        // line should be one of these two:
                        // -r -s n -f filepath
                        // -r -s n -f filepath\tbundle:bundlename
                        // where
                        // n is the assetstore number
                        // filepath is the path of the file to be registered
                        // bundlename is an optional bundle name
                        String sRegistrationLine = line.trim();
                        int iAssetstore = -1;
                        String sFilePath = null;
                        String sBundle = null;
                        StringTokenizer tokenizer = new StringTokenizer(sRegistrationLine);
                        while (tokenizer.hasMoreTokens()) {
                            String sToken = tokenizer.nextToken();
                            if ("-r".equals(sToken)) {
                                continue;
                            } else if ("-s".equals(sToken) && tokenizer.hasMoreTokens()) {
                                try {
                                    iAssetstore = Integer.parseInt(tokenizer.nextToken());
                                } catch (NumberFormatException e) {
                                    // ignore - iAssetstore remains -1
                                }
                            } else if ("-f".equals(sToken) && tokenizer.hasMoreTokens()) {
                                sFilePath = tokenizer.nextToken();
                            } else if (sToken.startsWith("bundle:")) {
                                sBundle = sToken.substring(7);
                            } else {
                                // unrecognized token - should be no problem
                            }
                        } // while
                        if (iAssetstore == -1 || sFilePath == null) {
                            System.out.println("\tERROR: invalid contents file line");
                            System.out.println("\t\tSkipping line: "
                                    + sRegistrationLine);
                            continue;
                        }

                        // look for descriptions
                        boolean descriptionExists = false;
                        String descriptionMarker = "\tdescription:";
                        int dMarkerIndex = line.indexOf(descriptionMarker);
                        int dEndIndex = 0;
                        if (dMarkerIndex > 0) {
                            dEndIndex = line.indexOf("\t", dMarkerIndex + 1);
                            if (dEndIndex == -1) {
                                dEndIndex = line.length();
                            }
                            descriptionExists = true;
                        }
                        String sDescription = "";
                        if (descriptionExists) {
                            sDescription = line.substring(dMarkerIndex, dEndIndex);
                            sDescription = sDescription.replaceFirst("description:", "");
                        }

                        registerBitstream(c, i, iAssetstore, sFilePath, sBundle, sDescription);
                        System.out.println("\tRegistering Bitstream: " + sFilePath
                                + "\tAssetstore: " + iAssetstore
                                + "\tBundle: " + sBundle
                                + "\tDescription: " + sDescription);
                        continue; // process next line in contents file
                    }

                    int bitstreamEndIndex = line.indexOf('\t');

                    if (bitstreamEndIndex == -1) {
                        // no extra info
                        processContentFileEntry(c, i, path, line, null, false);
                        System.out.println("\tBitstream: " + line);
                    } else {

                        String bitstreamName = line.substring(0, bitstreamEndIndex);

                        boolean bundleExists = false;
                        boolean permissionsExist = false;
                        boolean descriptionExists = false;
                        boolean labelExists = false;
                        boolean heightExists = false;
                        boolean widthExists = false;
                        boolean tocExists = false;

                        // look for label
                        String labelMarker = "\tiiif-label";
                        int lMarkerIndex = line.indexOf(labelMarker);
                        int lEndIndex = 0;
                        if (lMarkerIndex > 0) {
                            lEndIndex = line.indexOf("\t", lMarkerIndex + 1);
                            if (lEndIndex == -1) {
                                lEndIndex = line.length();
                            }
                            labelExists = true;
                        }

                        // look for height
                        String heightMarker = "\tiiif-height";
                        int hMarkerIndex = line.indexOf(heightMarker);
                        int hEndIndex = 0;
                        if (hMarkerIndex > 0) {
                            hEndIndex = line.indexOf("\t", hMarkerIndex + 1);
                            if (hEndIndex == -1) {
                                hEndIndex = line.length();
                            }
                            heightExists = true;
                        }

                        // look for width
                        String widthMarker = "\tiiif-width";
                        int wMarkerIndex = line.indexOf(widthMarker);
                        int wEndIndex = 0;
                        if (wMarkerIndex > 0) {
                            wEndIndex = line.indexOf("\t", wMarkerIndex + 1);
                            if (wEndIndex == -1) {
                                wEndIndex = line.length();
                            }
                            widthExists = true;
                        }

                        // look for toc
                        String tocMarker = "\tiiif-toc";
                        int tMarkerIndex = line.indexOf(tocMarker);
                        int tEndIndex = 0;
                        if (tMarkerIndex > 0) {
                            tEndIndex = line.indexOf("\t", tMarkerIndex + 1);
                            if (tEndIndex == -1) {
                                tEndIndex = line.length();
                            }
                            tocExists = true;
                        }

                        // look for a bundle name
                        String bundleMarker = "\tbundle:";
                        int bMarkerIndex = line.indexOf(bundleMarker);
                        int bEndIndex = 0;
                        if (bMarkerIndex > 0) {
                            bEndIndex = line.indexOf("\t", bMarkerIndex + 1);
                            if (bEndIndex == -1) {
                                bEndIndex = line.length();
                            }
                            bundleExists = true;
                        }

                        // look for permissions
                        String permissionsMarker = "\tpermissions:";
                        int pMarkerIndex = line.indexOf(permissionsMarker);
                        int pEndIndex = 0;
                        if (pMarkerIndex > 0) {
                            pEndIndex = line.indexOf("\t", pMarkerIndex + 1);
                            if (pEndIndex == -1) {
                                pEndIndex = line.length();
                            }
                            permissionsExist = true;
                        }

                        // look for descriptions
                        String descriptionMarker = "\tdescription:";
                        int dMarkerIndex = line.indexOf(descriptionMarker);
                        int dEndIndex = 0;
                        if (dMarkerIndex > 0) {
                            dEndIndex = line.indexOf("\t", dMarkerIndex + 1);
                            if (dEndIndex == -1) {
                                dEndIndex = line.length();
                            }
                            descriptionExists = true;
                        }

                        // is this the primary bitstream?
                        String primaryBitstreamMarker = "\tprimary:true";
                        boolean primary = false;
                        String primaryStr = "";
                        if (line.contains(primaryBitstreamMarker)) {
                            primary = true;
                            primaryStr = "\t **Setting as primary bitstream**";
                        }

                        if (bundleExists) {
                            String bundleName = line.substring(bMarkerIndex
                                    + bundleMarker.length(), bEndIndex).trim();

                            processContentFileEntry(c, i, path, bitstreamName, bundleName, primary);
                            System.out.println("\tBitstream: " + bitstreamName +
                                    "\tBundle: " + bundleName +
                                    primaryStr);
                        } else {
                            processContentFileEntry(c, i, path, bitstreamName, null, primary);
                            System.out.println("\tBitstream: " + bitstreamName + primaryStr);
                        }

                        if (permissionsExist || descriptionExists || labelExists || heightExists
                                || widthExists || tocExists) {
                            System.out.println("Gathering options.");
                            String extraInfo = bitstreamName;

                            if (permissionsExist) {
                                extraInfo = extraInfo
                                        + line.substring(pMarkerIndex, pEndIndex);
                            }

                            if (descriptionExists) {
                                extraInfo = extraInfo
                                        + line.substring(dMarkerIndex, dEndIndex);
                            }

                            if (labelExists) {
                                extraInfo = extraInfo
                                        + line.substring(lMarkerIndex, lEndIndex);
                            }

                            if (heightExists) {
                                extraInfo = extraInfo
                                        + line.substring(hMarkerIndex, hEndIndex);
                            }

                            if (widthExists) {
                                extraInfo = extraInfo
                                        + line.substring(wMarkerIndex, wEndIndex);
                            }

                            if (tocExists) {
                                extraInfo = extraInfo
                                        + line.substring(tMarkerIndex, tEndIndex);
                            }

                            options.add(extraInfo);
                        }
                    }
                }
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        } else {
            File dir = new File(path);
            String[] dirListing = dir.list();
            for (String fileName : dirListing) {
                if (!"dublin_core.xml".equals(fileName) && !fileName.equals("handle") && !metadataFileFilter
                        .accept(dir, fileName)) {
                    throw new FileNotFoundException("No contents file found");
                }
            }

            System.out.println("No contents file found - but only metadata files found. Assuming metadata only.");
        }

        return options;
    }

    /**
     * each entry represents a bitstream....
     *
     * @param c          DSpace Context
     * @param i          Dspace Item
     * @param path       path to file
     * @param fileName   file name
     * @param bundleName bundle name
     * @param primary    if primary bitstream
     * @throws SQLException       if database error
     * @throws IOException        if IO error
     * @throws AuthorizeException if authorization error
     */
    protected void processContentFileEntry(Context c, Item i, String path,
            String fileName, String bundleName, boolean primary) throws SQLException,
            IOException, AuthorizeException {
        String fullpath = path + File.separatorChar + fileName;

        // get an input stream
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(
                fullpath));

        Bitstream bs = null;
        String newBundleName = bundleName;

        if (bundleName == null) {
            // is it license.txt?
            if ("license.txt".equals(fileName)) {
                newBundleName = "LICENSE";
            } else {
                // call it ORIGINAL
                newBundleName = "ORIGINAL";
            }
        }

        if (!isTest) {
            // find the bundle
            List<Bundle> bundles = itemService.getBundles(i, newBundleName);
            Bundle targetBundle = null;

            if (bundles.size() < 1) {
                // not found, create a new one
                targetBundle = bundleService.create(c, i, newBundleName);
            } else {
                // put bitstreams into first bundle
                targetBundle = bundles.iterator().next();
            }

            // now add the bitstream
            bs = bitstreamService.create(c, targetBundle, bis);

            bs.setName(c, fileName);

            // Identify the format
            // FIXME - guessing format guesses license.txt incorrectly as a text
            // file format!
            BitstreamFormat bf = bitstreamFormatService.guessFormat(c, bs);
            bitstreamService.setFormat(c, bs, bf);

            // Is this a the primary bitstream?
            if (primary) {
                targetBundle.setPrimaryBitstreamID(bs);
                bundleService.update(c, targetBundle);
            }

            bitstreamService.update(c, bs);
        }

        bis.close();
    }

    /**
     * Register the bitstream file into DSpace
     *
     * @param c             DSpace Context
     * @param i             DSpace Item
     * @param assetstore    assetstore number
     * @param bitstreamPath the full filepath expressed in the contents file
     * @param bundleName    bundle name
     * @param description   bitstream description
     * @throws SQLException       if database error
     * @throws IOException        if IO error
     * @throws AuthorizeException if authorization error
     */
    protected void registerBitstream(Context c, Item i, int assetstore,
            String bitstreamPath, String bundleName, String description)
            throws SQLException, IOException, AuthorizeException {
        // TODO validate assetstore number
        // TODO make sure the bitstream is there

        Bitstream bs = null;
        String newBundleName = bundleName;

        if (StringUtils.isBlank(bundleName)) {
            // is it license.txt?
            if (bitstreamPath.endsWith("license.txt")) {
                newBundleName = "LICENSE";
            } else {
                // call it ORIGINAL
                newBundleName = "ORIGINAL";
            }
        }

        if (!isTest) {
            // find the bundle
            List<Bundle> bundles = itemService.getBundles(i, newBundleName);
            Bundle targetBundle = null;

            if (bundles.size() < 1) {
                // not found, create a new one
                targetBundle = bundleService.create(c, i, newBundleName);
            } else {
                // put bitstreams into first bundle
                targetBundle = bundles.iterator().next();
            }

            // now add the bitstream
            bs = bitstreamService.register(c, targetBundle, assetstore, bitstreamPath);

            // set the name to just the filename
            int iLastSlash = bitstreamPath.lastIndexOf('/');
            bs.setName(c, bitstreamPath.substring(iLastSlash + 1));

            // Identify the format
            // FIXME - guessing format guesses license.txt incorrectly as a text file
            // format!
            BitstreamFormat bf = bitstreamFormatService.guessFormat(c, bs);
            bitstreamService.setFormat(c, bs, bf);
            bs.setDescription(c, description);

            bitstreamService.update(c, bs);
        }
    }

    /**
     * Process the Options to apply to the Item. The options are tab delimited
     *
     * Options:
     * {@code
     * 48217870-MIT.pdf        permissions: -r 'MIT Users'     description: Full printable version (MIT only)
     * permissions:[r|w]-['group name']
     * description: 'the description of the file'
     * }
     * where:
     * {@code
     * [r|w] (meaning: read|write)
     * ['MIT Users'] (the group name)
     * }
     *
     * @param c       DSpace Context
     * @param myItem  DSpace Item
     * @param options List of option strings
     * @throws SQLException       if database error
     * @throws AuthorizeException if authorization error
     */
    protected void processOptions(Context c, Item myItem, List<String> options)
            throws SQLException, AuthorizeException {
        System.out.println("Processing options.");
        for (String line : options) {
            System.out.println("\tprocessing " + line);

            boolean permissionsExist = false;
            boolean descriptionExists = false;
            boolean labelExists = false;
            boolean heightExists = false;
            boolean widthExists = false;
            boolean tocExists = false;

            String permissionsMarker = "\tpermissions:";
            int pMarkerIndex = line.indexOf(permissionsMarker);
            int pEndIndex = 0;
            if (pMarkerIndex > 0) {
                pEndIndex = line.indexOf("\t", pMarkerIndex + 1);
                if (pEndIndex == -1) {
                    pEndIndex = line.length();
                }
                permissionsExist = true;
            }

            String descriptionMarker = "\tdescription:";
            int dMarkerIndex = line.indexOf(descriptionMarker);
            int dEndIndex = 0;
            if (dMarkerIndex > 0) {
                dEndIndex = line.indexOf("\t", dMarkerIndex + 1);
                if (dEndIndex == -1) {
                    dEndIndex = line.length();
                }
                descriptionExists = true;
            }

            // look for label
            String labelMarker = "\tiiif-label:";
            int lMarkerIndex = line.indexOf(labelMarker);
            int lEndIndex = 0;
            if (lMarkerIndex > 0) {
                lEndIndex = line.indexOf("\t", lMarkerIndex + 1);
                if (lEndIndex == -1) {
                    lEndIndex = line.length();
                }
                labelExists = true;
            }

            // look for height
            String heightMarker = "\tiiif-height:";
            int hMarkerIndex = line.indexOf(heightMarker);
            int hEndIndex = 0;
            if (hMarkerIndex > 0) {
                hEndIndex = line.indexOf("\t", hMarkerIndex + 1);
                if (hEndIndex == -1) {
                    hEndIndex = line.length();
                }
                heightExists = true;
            }

            // look for width
            String widthMarker = "\tiiif-width:";
            int wMarkerIndex = line.indexOf(widthMarker);
            int wEndIndex = 0;
            if (wMarkerIndex > 0) {
                wEndIndex = line.indexOf("\t", wMarkerIndex + 1);
                if (wEndIndex == -1) {
                    wEndIndex = line.length();
                }
                widthExists = true;
            }

            // look for toc
            String tocMarker = "\tiiif-toc:";
            int tMarkerIndex = line.indexOf(tocMarker);
            int tEndIndex = 0;
            if (tMarkerIndex > 0) {
                tEndIndex = line.indexOf("\t", tMarkerIndex + 1);
                if (tEndIndex == -1) {
                    tEndIndex = line.length();
                }
                tocExists = true;
            }

            int bsEndIndex = line.indexOf("\t");
            String bitstreamName = line.substring(0, bsEndIndex);

            int actionID = -1;
            String groupName = "";
            Group myGroup = null;
            if (permissionsExist) {
                String thisPermission = line.substring(pMarkerIndex
                        + permissionsMarker.length(), pEndIndex);

                // get permission type ("read" or "write")
                int pTypeIndex = thisPermission.indexOf('-');

                // get permission group (should be in single quotes)
                int groupIndex = thisPermission.indexOf('\'', pTypeIndex);
                int groupEndIndex = thisPermission.indexOf('\'', groupIndex + 1);

                // if not in single quotes, assume everything after type flag is
                // group name
                if (groupIndex == -1) {
                    groupIndex = thisPermission.indexOf(' ', pTypeIndex);
                    groupEndIndex = thisPermission.length();
                }

                groupName = thisPermission.substring(groupIndex + 1,
                        groupEndIndex);

                if (thisPermission.toLowerCase().charAt(pTypeIndex + 1) == 'r') {
                    actionID = Constants.READ;
                } else if (thisPermission.toLowerCase().charAt(pTypeIndex + 1) == 'w') {
                    actionID = Constants.WRITE;
                }

                try {
                    myGroup = groupService.findByName(c, groupName);
                } catch (SQLException sqle) {
                    System.out.println("SQL Exception finding group name: "
                            + groupName);
                    // do nothing, will check for null group later
                }
            }

            String thisDescription = "";
            if (descriptionExists) {
                thisDescription = line.substring(
                        dMarkerIndex + descriptionMarker.length(), dEndIndex)
                        .trim();
            }

            String thisLabel = "";
            if (labelExists) {
                thisLabel = line.substring(
                        lMarkerIndex + labelMarker.length(), lEndIndex)
                        .trim();
            }

            String thisHeight = "";
            if (heightExists) {
                thisHeight = line.substring(
                        hMarkerIndex + heightMarker.length(), hEndIndex)
                        .trim();
            }

            String thisWidth = "";
            if (widthExists) {
                thisWidth = line.substring(
                        wMarkerIndex + widthMarker.length(), wEndIndex)
                        .trim();
            }

            String thisToc = "";
            if (tocExists) {
                thisToc = line.substring(
                        tMarkerIndex + tocMarker.length(), tEndIndex)
                        .trim();
            }

            Bitstream bs = null;
            boolean notfound = true;
            boolean updateRequired = false;

            if (!isTest) {
                // find bitstream
                List<Bitstream> bitstreams = itemService.getNonInternalBitstreams(c, myItem);
                for (int j = 0; j < bitstreams.size() && notfound; j++) {
                    if (bitstreams.get(j).getName().equals(bitstreamName)) {
                        bs = bitstreams.get(j);
                        notfound = false;
                    }
                }
            }

            if (notfound && !isTest) {
                // this should never happen
                System.out.println("\tdefault permissions set for "
                        + bitstreamName);
            } else if (!isTest) {
                if (permissionsExist) {
                    if (myGroup == null) {
                        System.out.println("\t" + groupName
                                + " not found, permissions set to default");
                    } else if (actionID == -1) {
                        System.out
                                .println("\tinvalid permissions flag, permissions set to default");
                    } else {
                        System.out.println("\tSetting special permissions for "
                                + bitstreamName);
                        setPermission(c, myGroup, actionID, bs);
                    }
                }

                if (descriptionExists) {
                    System.out.println("\tSetting description for "
                            + bitstreamName);
                    bs.setDescription(c, thisDescription);
                    updateRequired = true;
                }

                if (labelExists) {
                    MetadataField metadataField = metadataFieldService
                            .findByElement(c, METADATA_IIIF_SCHEMA, METADATA_IIIF_LABEL_ELEMENT, null);
                    System.out.println("\tSetting label to " + thisLabel + " in element "
                            + metadataField.getElement() + " on " + bitstreamName);
                    bitstreamService.addMetadata(c, bs, metadataField, null, thisLabel);
                    updateRequired = true;
                }

                if (heightExists) {
                    MetadataField metadataField = metadataFieldService
                            .findByElement(c, METADATA_IIIF_SCHEMA, METADATA_IIIF_IMAGE_ELEMENT,
                                    METADATA_IIIF_HEIGHT_QUALIFIER);
                    System.out.println("\tSetting height to " + thisHeight + " in element "
                            + metadataField.getElement() + " on " + bitstreamName);
                    bitstreamService.addMetadata(c, bs, metadataField, null, thisHeight);
                    updateRequired = true;
                }
                if (widthExists) {
                    MetadataField metadataField = metadataFieldService
                            .findByElement(c, METADATA_IIIF_SCHEMA, METADATA_IIIF_IMAGE_ELEMENT,
                                    METADATA_IIIF_WIDTH_QUALIFIER);
                    System.out.println("\tSetting width to " + thisWidth + " in element "
                            + metadataField.getElement() + " on " + bitstreamName);
                    bitstreamService.addMetadata(c, bs, metadataField, null, thisWidth);
                    updateRequired = true;
                }
                if (tocExists) {
                    MetadataField metadataField = metadataFieldService
                            .findByElement(c, METADATA_IIIF_SCHEMA, METADATA_IIIF_TOC_ELEMENT, null);
                    System.out.println("\tSetting toc to " + thisToc + " in element "
                            + metadataField.getElement() + " on " + bitstreamName);
                    bitstreamService.addMetadata(c, bs, metadataField, null, thisToc);
                    updateRequired = true;
                }
                if (updateRequired) {
                    bitstreamService.update(c, bs);
                }
            }
        }
    }

    /**
     * Set the Permission on a Bitstream.
     *
     * @param c        DSpace Context
     * @param g        Dspace Group
     * @param actionID action identifier
     * @param bs       Bitstream
     * @throws SQLException       if database error
     * @throws AuthorizeException if authorization error
     * @see org.dspace.core.Constants
     */
    protected void setPermission(Context c, Group g, int actionID, Bitstream bs)
            throws SQLException, AuthorizeException {
        if (!isTest) {
            // remove the default policy
            authorizeService.removeAllPolicies(c, bs);

            // add the policy
            ResourcePolicy rp = resourcePolicyService.create(c);

            rp.setdSpaceObject(bs);
            rp.setAction(actionID);
            rp.setGroup(g);

            resourcePolicyService.update(c, rp);
        } else {
            if (actionID == Constants.READ) {
                System.out.println("\t\tpermissions: READ for " + g.getName());
            } else if (actionID == Constants.WRITE) {
                System.out.println("\t\tpermissions: WRITE for " + g.getName());
            }
        }

    }

    // XML utility methods

    /**
     * Lookup an attribute from a DOM node.
     *
     * @param n    node
     * @param name attribute name
     * @return attribute value
     */
    private String getAttributeValue(Node n, String name) {
        NamedNodeMap nm = n.getAttributes();

        for (int i = 0; i < nm.getLength(); i++) {
            Node node = nm.item(i);

            if (name.equals(node.getNodeName())) {
                return node.getNodeValue();
            }
        }

        return "";
    }

    /**
     * Return the String value of a Node.
     *
     * @param node node
     * @return string value
     */
    protected String getStringValue(Node node) {
        String value = node.getNodeValue();

        if (node.hasChildNodes()) {
            Node first = node.getFirstChild();

            if (first.getNodeType() == Node.TEXT_NODE) {
                return first.getNodeValue();
            }
        }

        return value;
    }

    /**
     * Load in the XML from file.
     *
     * @param filename the filename to load from
     * @return the DOM representation of the XML file
     * @throws IOException                  if IO error
     * @throws ParserConfigurationException if config error
     * @throws SAXException                 if XML error
     */
    protected Document loadXML(String filename) throws IOException,
            ParserConfigurationException, SAXException {
        DocumentBuilder builder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();

        return builder.parse(new File(filename));
    }

    /**
     * Delete a directory and its child files and directories
     *
     * @param path The directory to delete
     * @return Whether the deletion was successful or not
     */
    protected boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                } else {
                    if (!files[i].delete()) {
                        log.error("Unable to delete file: " + files[i].getName());
                    }
                }
            }
        }

        boolean pathDeleted = path.delete();
        return (pathDeleted);
    }

    /**
     * Generate a random filename based on current time
     *
     * @param hidden set to add . as a prefix to make the file hidden
     * @return the filename
     */
    protected String generateRandomFilename(boolean hidden) {
        String filename = String.format("%s", RandomStringUtils.randomAlphanumeric(8));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmm");
        String datePart = sdf.format(new Date());
        filename = datePart + "_" + filename;

        return filename;
    }

    @Override
    public void setTest(boolean isTest) {
        this.isTest = isTest;
    }

    @Override
    public void setResume(boolean isResume) {
        this.isResume = isResume;
    }

    @Override
    public void setUseWorkflow(boolean useWorkflow) {
        this.useWorkflow = useWorkflow;
    }

    @Override
    public void setUseWorkflowSendEmail(boolean useWorkflowSendEmail) {
        this.useWorkflowSendEmail = useWorkflowSendEmail;
    }

    @Override
    public void setQuiet(boolean isQuiet) {
        this.isQuiet = isQuiet;
    }

}
