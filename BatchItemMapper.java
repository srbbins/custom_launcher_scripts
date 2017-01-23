package uiuc.edu.dspace.app.batchItemMapper;

import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.dspace.app.xmlui.aspect.administrative.FlowItemUtils;
import org.dspace.app.xmlui.aspect.administrative.FlowMapperUtils;
import org.dspace.app.xmlui.utils.UIException;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Collection;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;
import org.dspace.handle.HandleManager;
import org.dspace.identifier.Handle;
import org.dspace.search.DSIndexer;

import java.io.*;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by srobbins on 8/9/15.
 */
public class BatchItemMapper {
    private final Context context;
    private final String inFileName;
    private final String outFileName;
    private final Map<String, String> fileMap;
    private final MapperMode mode;

    private BatchItemMapper(Context c, String inFileName, String outFileName, MapperMode mode) throws Exception {
        this.context = c;
        this.inFileName = inFileName;
        this.outFileName = outFileName;
        this.mode = mode;
        this.fileMap = readArgFile();
    }

    public static void main(String[] argv){
        DSIndexer.setBatchProcessingMode(true);
        Date startTime = new Date();
        int status = 0;
        try
        {
            // create an options object and populate it
            CommandLineParser parser = new PosixParser();
            Options options = new Options();
            options.addOption("m", "map", false, "map mode");
            options.addOption("u", "unmap", false, "unmap mode");
            options.addOption("M","move",false,"move mode");
            options.addOption("f", "file", true, "file containing items metdatavalue changes");
            options.addOption("e", "eperson", true, "eperson to perform update actions");
            options.addOption("r", "rollback", true, "name of rollback file which contains the item and the previous owning collection");
            options.addOption("v", "verbose", false, "verbose logging to stdout");
            CommandLine line = parser.parse(options, argv);
            // create a context
            Context c = new Context();
            String eperson = line.getOptionValue("e");
            EPerson myEPerson = parseUser(c, line.getOptionValue("e"));
            String in = line.getOptionValue("f");
            String out = line.getOptionValue("r");


            c.setCurrentUser(myEPerson);
            BatchItemMapper mapper = new BatchItemMapper(c, in, out, parseMode(line));
            mapper.process();

        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println(e);
            status = 1;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
            status = 1;
        } finally
        {
            DSIndexer.setBatchProcessingMode(false);
            Date endTime = new Date();
            System.out.println("Started: " + startTime.getTime());
            System.out.println("Ended: " + endTime.getTime());
            System.out.println("Elapsed time: " + ((endTime.getTime() - startTime.getTime()) / 1000) + " secs (" + (endTime.getTime() - startTime.getTime()) + " msecs)");
        }

        System.exit(status);
    }

    private static MapperMode parseMode(CommandLine options) {
        checkMode(options);
        if(options.hasOption("m")){
            return MapperMode.MAP;
        }
        else if(options.hasOption("M")){
            return MapperMode.MOVE;
        }
        else if(options.hasOption("u")){
            return MapperMode.UNMAP;
        }
        System.out.println("Mode validation failed, check code for bugs!");
        return null;
    }

    private static void checkMode(CommandLine options) {
        int modeCount = 0;
        for( Object option: options.getOptions()){
            Option castedOption = (Option)option;
        }
        if(options.hasOption("m")){
            modeCount++;
        }
        if(options.hasOption("M")){
            modeCount++;
        }
        if(options.hasOption("u")){
            modeCount++;
        }
        System.out.print(modeCount);
        if(modeCount!=1){
            System.out.println("options must have one and only one of either \"m,\" \"M,\", or \"u\"");
            System.exit(1);
        }
    }
    private void processFileMap() throws SQLException, IOException, AuthorizeException, UIException {
        PrintWriter out = new PrintWriter(outFileName, Constants.DEFAULT_ENCODING);
        final CSVPrinter printer = CSVFormat.DEFAULT.withHeader("item_id", "previous_owning_collection").print(out);

        try {
            //TODO refactor into overridden method
            for (String item_handle : fileMap.keySet()) {
                Item item = getItemFromHandle(item_handle);
                writeRollback(printer, item);
                if (mode == MapperMode.MAP) {
                    mapItem(item, fileMap.get(item_handle));
                } else if (mode == MapperMode.MOVE) {
                    moveItem(item, fileMap.get(item_handle));
                } else if (mode == MapperMode.UNMAP) {
                    unmapItem(item, fileMap.get(item_handle));
                }
            }
        }finally {
            printer.close();
        }
    }

    private void writeRollback(CSVPrinter printer, Item item) throws SQLException, IOException {
        if(item.getOwningCollection()!=null) {
            printer.printRecord(item.getHandle(), item.getOwningCollection().getHandle());
        } else {
            printer.printRecord(item.getHandle(), "no owning collection");
        }
        printer.flush();
    }

    private Collection getCollectionFromHandle(String collectionHandle) throws SQLException {
        DSpaceObject dso = HandleManager.resolveToObject(context, collectionHandle);
        if (!(dso instanceof Collection)){
            System.out.printf("Handle %s does not resolve to collection.\n", collectionHandle);
            System.exit(1);
        }
        return (Collection) dso;
    }

    private Item getItemFromHandle(String item_handle) throws SQLException {
        DSpaceObject dso = HandleManager.resolveToObject(context, item_handle);
        if (!(dso instanceof Item)){
            System.out.printf("Handle %s does not resolve to item.\n", item_handle);
            System.exit(1);
        }
        return (Item)dso;
    }

    private void unmapItem(Item item, String collectionHandle) throws SQLException, IOException, AuthorizeException, UIException {
        FlowMapperUtils.processUnmapItems(context, getCollectionFromHandle(collectionHandle).getID(), new String[]{String.valueOf(item.getID())});
    }



    private void moveItem(Item item, String collectionHandle) throws SQLException, IOException, AuthorizeException {
        FlowItemUtils.processMoveItem(context, item.getID(), getCollectionFromHandle(collectionHandle).getID(), false);
    }

    private void mapItem(Item item, String collectionHandle) throws SQLException, UIException, IOException, AuthorizeException {
        String[] handleArg = new String[]{String.valueOf(item.getID())};
        Collection destination = getCollectionFromHandle(collectionHandle);
        FlowMapperUtils.processMapItems(context, destination.getID(), handleArg);
    }

    private static EPerson parseUser(Context c, String eperson) throws SQLException, AuthorizeException {
        // find the EPerson, assign to context
        EPerson myEPerson =null;
        if (eperson.indexOf('@') != -1)
        {
            // @ sign, must be an email
            myEPerson = EPerson.findByEmail(c, eperson);
        }
        else
        {
            myEPerson = EPerson.find(c, Integer.parseInt(eperson));
        }

        if (myEPerson == null)
        {
            System.out.println("Error, eperson cannot be found: " + eperson);
            System.exit(1);
        }
        return myEPerson;
    }

    private void process() throws Exception {

        testFiles();
        processFileMap();
    }
    ////////////////////////////////////
    // utility methods
    ////////////////////////////////////
    // read in the map file and generate a hashmap of (file,handle) pairs
    private Map<String, String> readArgFile() throws Exception
    {
        Map<String, String> myHash = new HashMap<String, String>();
        Reader in = new FileReader(inFileName);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().parse(in);
        for (CSVRecord record : records) {
            String itemHandle;
            String collectionHandle;
            itemHandle = record.get("item_handle");
            collectionHandle = record.get("collection_handle");
            myHash.put(itemHandle, collectionHandle);
        }
        return myHash;
    }

    private void testFiles(){
        boolean exit = false;
        if (!(new File(inFileName).exists())){
            exit = true;
            System.out.printf("Input csv %s not found\n", inFileName);
        }
        try{
            new File(outFileName).createNewFile();
        }
        catch(IOException e){
            exit = true;
            System.out.printf("Output csv %s not found\n", outFileName);
        }
        if(exit) {
            System.exit(1);
        }
    }

    private enum MapperMode{
        MAP, MOVE, UNMAP
    }
}
