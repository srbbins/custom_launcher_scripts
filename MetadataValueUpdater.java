package uiuc.edu.dspace.app.metadatavalueimport;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.MetadataValue;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;
import org.dspace.event.Event;
import org.dspace.search.DSIndexer;

import java.io.*;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.text.Normalizer;

/**
 * Created by srobbins on 7/24/15.
 * Launcher task to update metadatavalue based on Database ids.
 * A csv generated from the database can be modified in place
 * and fed to this batch process.
 */
public class MetadataValueUpdater {
    private final Context context;
    private final String inFileName;
    private final String outFileName;
    private final Iterable<CSVRecord> records;
    private final boolean isVerbose;
    private static CSVPrinter printer = null;//CSVFormat.DEFAULT.withHeader("metadata_value_id", "text_value");


    public MetadataValueUpdater(Context c, String in, String out, boolean isVerbose) throws Exception {
        this.inFileName = in;
        this.outFileName = out;
        this.context = c;
        this.records = readArgFile();
        this.isVerbose = isVerbose;
    }

    public static void main(String[] argv) throws IOException {
        DSIndexer.setBatchProcessingMode(true);
        Date startTime = new Date();
        int status = 0;
        try
        {
            // create an options object and populate it
            CommandLineParser parser = new PosixParser();
            Options options = new Options();
            options.addOption("f", "file", true, "file containing items metdatavalue changes");
            options.addOption("e", "eperson", true, "eperson to perform update actions");
            options.addOption("r", "rollback", true, "name of rollback file");
            options.addOption("v", "verbose", false, "verbose logging to stdout");
            options.addOption("h", "help", false, "help");
            CommandLine line = parser.parse(options, argv);

            if (line.hasOption('h')){
                printHelpAndExit(options);
            }
            // create a context
            Context c = new Context();
            EPerson myEPerson = parseUser(c, line.getOptionValue("e"));
            String in = line.getOptionValue("f");
            String out = line.getOptionValue("r");

            PrintWriter printerStream = new PrintWriter(out, Constants.DEFAULT_ENCODING);
            printer = CSVFormat.DEFAULT.withHeader("metadata_value_id", "text_value", "item_id", "metadata_field_id").print(printerStream);
            c.setCurrentUser(myEPerson);
            MetadataValueUpdater updater = new MetadataValueUpdater(c, in, out, line.hasOption("v"));
            updater.process();

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
            printer.close();
            DSIndexer.setBatchProcessingMode(false);
            Date endTime = new Date();
            System.out.println("Started: " + startTime.getTime());
            System.out.println("Ended: " + endTime.getTime());
            System.out.println("Elapsed time: " + ((endTime.getTime() - startTime.getTime()) / 1000) + " secs (" + (endTime.getTime() - startTime.getTime()) + " msecs)");
        }

        System.exit(status);
    }

    private void process() throws IOException, SQLException, AuthorizeException {
        testFiles();
        processRecords();
    }

    private void processRecords() throws SQLException, IOException, AuthorizeException {
        for (CSVRecord record :records)
        {
            if (record.get("metadata_value_id").equals("")){
                printNewRecord(record);
                newRecord(record);

            } else {
                printProcessInfo(record);
                updateMDV(record);
            }
        }
    }

    private void newRecord(CSVRecord record) throws SQLException, AuthorizeException, IOException {
        MetadataValue metadata = new MetadataValue();
        metadata.setResourceId(Integer.parseInt(record.get("item_id")));
        metadata.setFieldId(Integer.parseInt(record.get("metadata_field_id")));
        metadata.setValue(record.get("text_value"));
        metadata.setLanguage("en");
        metadata.create(context);
        writeRollbackRowForNew(MetadataValue.find(context, metadata.getValueId()));
        context.addEvent(new Event(Event.MODIFY_METADATA, Constants.ITEM, metadata.getResourceId(), null));
        context.commit();
        context.clearCache();
    }

    private void writeRollbackRowForNew(MetadataValue mdv) throws IOException {
        printRollbackLine(String.valueOf(mdv.getValueId()), mdv.getValue());
        printer.printRecord(String.valueOf(mdv.getValueId()), "", mdv.getResourceId(), mdv.getFieldId());
        printer.flush();
    }


    private void writeRollbackRowForUpdate(MetadataValue mdv) throws IOException {
        printRollbackLine(String.valueOf(mdv.getValueId()), mdv.getValue());
        printer.printRecord(String.valueOf(mdv.getValueId()), mdv.getValue(), mdv.getResourceId(), mdv.getFieldId());
        printer.flush();
    }

    private void writeRollbackRowForDelete(MetadataValue mdv) throws IOException {
        printRollbackLine(String.valueOf(mdv.getValueId()), mdv.getValue());
        printer.printRecord("", mdv.getValue(), mdv.getResourceId(), mdv.getFieldId());
        printer.flush();
    }

    private static void printHelpAndExit(Options options) {
        HelpFormatter myHelp = new HelpFormatter();
        myHelp.printHelp(MetadataValueUpdater.class.toString(), options);
        System.exit(0);
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

    private void updateMDV(CSVRecord record) throws SQLException, IOException, AuthorizeException {
        String id = record.get("metadata_value_id");
        String newVal = getNewVal(record);
        MetadataValue mdv = MetadataValue.find(context, Integer.parseInt(id));
        if (mdv==null){
            System.out.printf("Skipping metadata_value_id %s. No metadatavalue found for %s\n", id, id);
            return;
        }
        if (getNewVal(record).equals("")){
            printDeleteVal(mdv);
            writeRollbackRowForDelete(mdv);
            mdv.delete(context);
            updateContextForMetadataChange(context, mdv.getResourceId());
        } else if (!rowCanBeSkipped(mdv, record)){
            printNewVal(newVal);
            writeRollbackRowForUpdate(mdv);
            mdv.setValue(newVal);
            if (record.isSet("metadata_field_id")){
                mdv.setFieldId(Integer.parseInt(record.get("metadata_field_id")));
            }
            mdv.update(context);
            updateContextForMetadataChange(context, mdv.getResourceId());
        }else {
            printOnSkip(id, mdv.getValue(), record);
        }

    }

    ////////////////////////////////////
    // utility methods
    ////////////////////////////////////

    private boolean rowCanBeSkipped(MetadataValue mdv, CSVRecord record){
        return mdv.getValue().equals(record.get("text_value"))&&mdv.getFieldId()==Integer.parseInt(record.get("metadata_field_id"));

    }

    // read in the map file and generate a hashmap of (file,handle) pairs
    private Iterable<CSVRecord> readArgFile() throws Exception
    {
        InputStream in = new FileInputStream(inFileName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().parse(reader);
        return records;
    }

    private void updateContextForMetadataChange(Context context, int item_id) throws SQLException {
        printOnChange(item_id);
        context.addEvent(new Event(Event.MODIFY_METADATA, Constants.ITEM, item_id, null));
        context.commit();
        context.clearCache();

    }

    private String getNewVal(CSVRecord record){
        return Normalizer.normalize(record.get("text_value"), Normalizer.Form.NFC);
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
    //verbose printing methods

    private void printOnChange(int id){
        if (isVerbose){
            System.out.printf("Changing Item id: %d\n", id);
        }
    }

    private void printNewVal(String newVal) {
        if (isVerbose){
            System.out.printf("newVal: %s\n", newVal);
        }
    }

    private void printOnSkip(String id, String value, CSVRecord record) {
         if (isVerbose){
             System.out.printf("skipping id %s since %s matched %s\n", id, record.get("metadata_value_id"), value);
         }
    }

    private void printRollbackLine(String metadataValueId, String textValue) {
        if (isVerbose){
            System.out.printf("writing key=%s, value=%s to rollback file\n", metadataValueId, textValue);
        }
    }


    private void printProcessInfo(CSVRecord record) {
        if (isVerbose) {
            System.out.println("key: " + record.get("metadata_value_id") + "; " + "val: "
                    + getNewVal(record));
        }
    }

    private void printNewRecord(CSVRecord record) {
        if (isVerbose) {
            System.out.println("new record for " + getNewVal(record) + "; ");
        }
    }
    private void printDeleteVal(MetadataValue mdv){
        if (isVerbose) {
            System.out.println("Deleting metadatavalue" + mdv.getValueId() + ". Field: "+mdv.getFieldId()+
                    " Value: "+mdv.getValue()+"; ");
        }
    }

}
