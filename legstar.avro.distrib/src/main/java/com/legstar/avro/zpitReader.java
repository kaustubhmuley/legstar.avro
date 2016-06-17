package com.legstar.avro.samples.zpitonly;

import java.io.File;
import java.io.FileInputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.legstar.avro.cob2avro.io.ZosVarRdwDatumReader;



public class zpitReader {
	// Avro schema for the zpit data record
    // Schema is generated by Cob2AvroTransGenerator
    private static final String AVRO_SHEMA_PATH = "gen/avsc/zpitonly.avsc";

    // Mainframe file on the file system
    // This one has a Record Descriptor Word (RDW) in front on each record
    private static final String ZOS_FILE_PATH = "data/ZPIT_22K_VB_NEW_FROM_MF.bin";

    public static void main(final String[] args) throws Exception {

    	System.out.print("ZipReader main started::");
        Schema schema = new Schema.Parser().parse(new File(AVRO_SHEMA_PATH));
       

        File inFile = new File(ZOS_FILE_PATH);
        System.out.print("ZipReader inFile created::");

        ZosVarRdwDatumReader <  ZpitRec > reader = new ZosVarRdwDatumReader < ZpitRec >(
                new FileInputStream(inFile), inFile.length(),
                new CobolZpitRec(), schema);
        
        
        System.out.print("ZipReader before next called::"+reader);
        DatumWriter<ZpitRec> zpitDatumWriter = new SpecificDatumWriter<ZpitRec>(ZpitRec.class);
        DataFileWriter<ZpitRec> zpitFileWriter = new DataFileWriter<ZpitRec>(zpitDatumWriter);
        System.out.print("ZipReader zpitFileWriter::"+zpitFileWriter);
        ZpitRec zpitRec = reader.next();
        zpitFileWriter.create(zpitRec.getSchema(), new File("/home/hari/zpitonlyData4.avro"));
       // ZpitRec zpitRec = reader.next();
    	zpitFileWriter.append(zpitRec);
    	   System.out.print("ZipReader after next called::" + zpitRec.getPitCreditData().getPitCityCntry().getPitCityName());
        
        while (reader.hasNext()) {
        	System.out.println("***Iterate 2nd Record::");
        		zpitFileWriter.append(reader.next());
        	
       }
        zpitFileWriter.flush();
        zpitFileWriter.close();
        reader.close();
        System.out.println("Customer data successfully serialized");
    }
}
