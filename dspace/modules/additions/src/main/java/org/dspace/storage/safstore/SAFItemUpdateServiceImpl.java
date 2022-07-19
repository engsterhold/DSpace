/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.storage.safstore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.Item;
import org.dspace.content.MetadataField;
import org.dspace.content.MetadataSchemaEnum;
import org.dspace.content.MetadataValue;
import org.dspace.content.service.BitstreamService;
import org.dspace.content.service.CommunityService;
import org.dspace.content.service.ItemService;
import org.dspace.core.Context;
import org.dspace.core.Utils;
import org.dspace.eperson.service.EPersonService;
import org.dspace.handle.service.HandleService;
import org.dspace.services.ConfigurationService;
import org.dspace.storage.safstore.service.SAFItemUpdateService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Item exporter to create simple AIPs for DSpace content. Currently exports
 * individual items, or entire collections. For instructions on use, see
 * printUsage() method.
 * <P>
 * ItemExport creates the simple AIP package that the importer also uses. It
 * consists of:
 * <P>
 * /exportdir/42/ (one directory per item) / dublin_core.xml - qualified dublin
 * core in RDF schema / contents - text file, listing one file per line / file1
 * - files contained in the item / file2 / ...
 * <P>
 * issues -doesn't handle special characters in metadata (needs to turn
 * {@code &'s} into
 * {@code &amp;}, etc.)
 * <P>
 * Modified by David Little, UCSD Libraries 12/21/04 to allow the registration
 * of files (bitstreams) into DSpace.
 *
 * @author David Little
 * @author Jay Paz
 */
public class SAFItemUpdateServiceImpl implements SAFItemUpdateService, InitializingBean {
    protected final int SUBDIR_LIMIT = 0;

    @Autowired(required = true)
    protected BitstreamService bitstreamService;
    @Autowired(required = true)
    protected CommunityService communityService;
    @Autowired(required = true)
    protected EPersonService ePersonService;
    @Autowired(required = true)
    protected ItemService itemService;
    @Autowired(required = true)
    protected HandleService handleService;
    @Autowired(required = true)
    protected ConfigurationService configurationService;

    /**
     * log4j logger
     */
    private final Logger log = org.apache.logging.log4j.LogManager.getLogger(SAFItemExportServiceImpl.class);
    protected String safStoreDir;
    protected final String REGISTERED_FLAG = "-R";

    @Override
    public void afterPropertiesSet() throws Exception {
        safStoreDir = configurationService.getProperty("safstore.dir");
        File safStoreDirFile = new File(safStoreDir);
        if (!(safStoreDirFile.exists() && safStoreDirFile.isDirectory())) {
            throw new FileNotFoundException(
                    "safStoreDir.dir is not set or does not point to a folder. safStoreDir: " + safStoreDir);
        }
    }

    protected SAFItemUpdateServiceImpl() {

    }

    @Override
    public void updateItem(Context c, Iterator<Item> i) throws Exception {
        Item myItem = i.next();

        List<Bitstream> b = myItem.getBundles().stream()
                .flatMap(s -> s.getBitstreams().stream())
                .collect(Collectors.toList());
        log.debug("First=" + b.get(0).getName() + " Bitstream_id=" + b.get(0).getInternalId());
        Set<String> a = b.stream().filter(s -> s.getInternalId().startsWith(
                REGISTERED_FLAG))
                .map(s -> Paths.get(s.getInternalId().substring(2)).getParent().toString())
                .collect(
                        Collectors.toSet());

        log.debug("Collected ref Strings:" + Arrays.toString(a.toArray()));
        String filePath = safStoreDir + System.getProperty("file.separator")
                + a.iterator().next();
        File itemDir = new File(filePath);
        if (itemDir.isDirectory()) {
            synchronized (this) {
                itemDir.setWritable(true, true);
                writeMetadata(c, myItem, itemDir, false);
                writeBitstreams(c, myItem, itemDir, true);
                writeHandle(c, myItem, itemDir);
                itemDir.setReadOnly();
            }
        } else {
            log.error(filePath + " is not a valid item directory.");
        }

    }

    /**
     * Discover the different schemas in use and output a separate metadata XML
     * file for each schema.
     *
     * @param c       DSpace context
     * @param i       DSpace Item
     * @param destDir destination directory
     * @param migrate Whether to use the migrate option or not
     * @throws Exception if error
     */
    protected void writeMetadata(Context c, Item i, File destDir, boolean migrate)
            throws Exception {
        Set<String> schemas = new HashSet<>();
        List<MetadataValue> dcValues = itemService.getMetadata(i, Item.ANY, Item.ANY, Item.ANY, Item.ANY);
        for (MetadataValue metadataValue : dcValues) {
            schemas.add(metadataValue.getMetadataField().getMetadataSchema().getName());
        }

        // Save each of the schemas into it's own metadata file
        for (String schema : schemas) {
            writeMetadata(c, schema, i, destDir, migrate);
        }
    }

    /**
     * output the item's dublin core into the item directory
     *
     * @param c       DSpace context
     * @param schema  schema
     * @param i       DSpace Item
     * @param destDir destination directory
     * @param migrate Whether to use the migrate option or not
     * @throws Exception if error
     */
    protected void writeMetadata(Context c, String schema, Item i,
            File destDir, boolean migrate) throws Exception {
        String filename;
        if (schema.equals(MetadataSchemaEnum.DC.getName())) {
            filename = "dublin_core.xml";
        } else {
            filename = "metadata_" + schema + ".xml";
        }

        File outFile = new File(destDir, filename);

        // System.out.println("Attempting to create file " + outFile);

        // if (outFile.createNewFile()) {
        BufferedOutputStream out = new BufferedOutputStream(
                new FileOutputStream(outFile));

        List<MetadataValue> dcorevalues = itemService.getMetadata(i, schema, Item.ANY, Item.ANY,
                Item.ANY);

        // XML preamble
        byte[] utf8 = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"no\"?>\n"
                .getBytes("UTF-8");
        out.write(utf8, 0, utf8.length);

        String dcTag = "<dublin_core schema=\"" + schema + "\">\n";
        utf8 = dcTag.getBytes("UTF-8");
        out.write(utf8, 0, utf8.length);

        String dateIssued = null;
        String dateAccessioned = null;

        for (MetadataValue dcv : dcorevalues) {
            MetadataField metadataField = dcv.getMetadataField();
            String qualifier = metadataField.getQualifier();

            if (qualifier == null) {
                qualifier = "none";
            }

            String language = dcv.getLanguage();

            if (language != null) {
                language = " language=\"" + language + "\"";
            } else {
                language = "";
            }

            utf8 = ("  <dcvalue element=\"" + metadataField.getElement() + "\" "
                    + "qualifier=\"" + qualifier + "\""
                    + language + ">"
                    + Utils.addEntities(dcv.getValue()) + "</dcvalue>\n")
                    .getBytes("UTF-8");

            if (!migrate ||
                    (migrate && !(("date".equals(metadataField.getElement()) && "issued".equals(qualifier)) ||
                            ("date".equals(metadataField.getElement()) && "accessioned".equals(qualifier)) ||
                            ("date".equals(metadataField.getElement()) && "available".equals(qualifier)) ||
                            ("identifier".equals(metadataField.getElement()) && "uri".equals(qualifier) &&
                                    (dcv.getValue() != null && dcv.getValue().startsWith(
                                            handleService.getCanonicalPrefix() + handleService.getPrefix() + "/")))
                            ||
                            ("description".equals(metadataField.getElement()) && "provenance".equals(qualifier)) ||
                            ("format".equals(metadataField.getElement()) && "extent".equals(qualifier)) ||
                            ("format".equals(metadataField.getElement()) && "mimetype".equals(qualifier))))) {
                out.write(utf8, 0, utf8.length);
            }

            // Store the date issued and accession to see if they are different
            // because we need to keep date.issued if they are, when migrating
            if (("date".equals(metadataField.getElement()) && "issued".equals(qualifier))) {
                dateIssued = dcv.getValue();
            }
            if (("date".equals(metadataField.getElement()) && "accessioned".equals(qualifier))) {
                dateAccessioned = dcv.getValue();
            }
        }

        // When migrating, only keep date.issued if it is different to date.accessioned
        if (migrate &&
                (dateIssued != null) &&
                (dateAccessioned != null) &&
                !dateIssued.equals(dateAccessioned)) {
            utf8 = ("  <dcvalue element=\"date\" "
                    + "qualifier=\"issued\">"
                    + Utils.addEntities(dateIssued) + "</dcvalue>\n")
                    .getBytes("UTF-8");
            out.write(utf8, 0, utf8.length);
        }

        utf8 = "</dublin_core>\n".getBytes("UTF-8");
        out.write(utf8, 0, utf8.length);

        out.close();
        // } else {
        // throw new Exception("Cannot create dublin_core.xml in " + destDir);
        // }
    }

    /**
     * create the file 'handle' which contains the handle assigned to the item
     *
     * @param c       DSpace Context
     * @param i       DSpace Item
     * @param destDir destination directory
     * @throws Exception if error
     */
    protected void writeHandle(Context c, Item i, File destDir)
            throws Exception {
        if (i.getHandle() == null) {
            return;
        }
        String filename = "handle";

        File outFile = new File(destDir, filename);

        // if (outFile.createNewFile()) {
        PrintWriter out = new PrintWriter(new FileWriter(outFile, StandardCharsets.UTF_8));

        out.println(i.getHandle());

        // close the contents file
        out.close();
        // } else {
        // throw new Exception("Cannot create file " + filename + " in "
        // + destDir);
        // }
    }

    /**
     * Create both the bitstreams and the contents file. Any bitstreams that
     * were originally registered will be marked in the contents file as such.
     * However, the export directory will contain actual copies of the content
     * files being exported.
     *
     * @param c                 the DSpace context
     * @param i                 the item being exported
     * @param destDir           the item's export directory
     * @param excludeBitstreams whether to exclude bitstreams
     * @throws Exception if error
     *                   if there is any problem writing to the export directory
     */
    protected void writeBitstreams(Context c, Item i, File destDir,
            boolean excludeBitstreams) throws Exception {
        File outFile = new File(destDir, "contents");

        // if (outFile.createNewFile()) {
        PrintWriter out = new PrintWriter(new FileWriter(outFile, StandardCharsets.UTF_8));

        List<Bundle> bundles = i.getBundles();

        for (Bundle bundle : bundles) {
            // bundles can have multiple bitstreams now...
            List<Bitstream> bitstreams = bundle.getBitstreams();

            String bundleName = bundle.getName();

            for (Bitstream bitstream : bitstreams) {
                String myName = bitstream.getName();
                String oldName = myName;

                String description = bitstream.getDescription();
                if (!StringUtils.isEmpty(description)) {
                    description = "\tdescription:" + description;
                } else {
                    description = "";
                }

                String primary = "";
                if (bitstream.equals(bundle.getPrimaryBitstream())) {
                    primary = "\tprimary:true ";
                }

                int myPrefix = 1; // only used with name conflict

                boolean isDone = false; // done when bitstream is finally
                // written

                while (!excludeBitstreams && !isDone) {
                    if (myName.contains(File.separator)) {
                        String dirs = myName.substring(0, myName
                                .lastIndexOf(File.separator));
                        File fdirs = new File(destDir + File.separator
                                + dirs);
                        if (!fdirs.exists() && !fdirs.mkdirs()) {
                            log.error("Unable to create destination directory");
                        }
                    }

                    File fout = new File(destDir, myName);

                    if (fout.createNewFile()) {
                        InputStream is = bitstreamService.retrieve(c, bitstream);
                        FileOutputStream fos = new FileOutputStream(fout);
                        Utils.bufferedCopy(is, fos);
                        // close streams
                        is.close();
                        fos.close();

                        isDone = true;
                    } else {
                        myName = myPrefix + "_" + oldName; // keep
                        // appending
                        // numbers to the
                        // filename until
                        // unique
                        myPrefix++;
                    }
                }

                // write the manifest file entry
                if (bitstreamService.isRegisteredBitstream(bitstream)) {

                    String outPath = bitstream.getInternalId().substring(2);

                    out.println("-r -s " + bitstream.getStoreNumber()
                            + " -f " + outPath +
                            "\tbundle:" + bundleName +
                            primary + description);
                } else {
                    out.println(myName + "\tbundle:" + bundleName +
                            primary + description);
                }

            }
        }

        // close the contents file
        out.close();
        // } else {
        // throw new Exception("Cannot create contents in " + destDir);
        // }
    }

}
