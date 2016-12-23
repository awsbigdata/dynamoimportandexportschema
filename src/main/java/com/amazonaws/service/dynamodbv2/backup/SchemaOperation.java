package com.amazonaws.service.dynamodbv2.backup;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.gson.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.Date;


/**
 * Created by srramas on 12/23/16.
 */
public class SchemaOperation {

    /**
     * Logger for the {@link SchemaOperation} class.
     */
    private static final Logger LOGGER = Logger.getLogger(SchemaOperation.class);

    public static void main(String args[]){

        // Initialize command line arguments and JCommander parser
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);

            // show usage information if help flag exists
            if (params.getHelp()) {
                cmd.usage();
                return;
            }

            AWSCredentialsProvider credentialsProvider=null;

            if(StringUtils.isNotEmpty(params.getProfileName())) {
                credentialsProvider = new ProfileCredentialsProvider(params.getProfileName());
            }
            else {
                // use default credential provider chain to locate appropriate credentials
                credentialsProvider = new DefaultAWSCredentialsProviderChain();
            }

            TableDescription tableDescription =null;
            //Load previous schema details

                if(StringUtils.isEmpty(params.getSchemaJson())){
                    if(StringUtils.isNoneEmpty(params.getSourceEndpoint()) &&
                            StringUtils.isNoneEmpty(params.getSourceTable())&&StringUtils.isNoneEmpty(params.getDestinationEndpoint()) &&
                            StringUtils.isNoneEmpty(params.getDestinationTable())){
                        tableDescription= getTablefromSourceDB(credentialsProvider,params);
                    }
                }else{
                     if(params.getType().equals("import"))
                    tableDescription=getTableFromSchema(params.getSchemaJson());
                    else if(params.getType().equals("export")){
                         if(StringUtils.isNoneEmpty(params.getSourceEndpoint()) &&
                                 StringUtils.isNoneEmpty(params.getSourceTable())){
                             tableDescription= getTablefromSourceDB(credentialsProvider,params);
                         }}
                        else{
                        cmd.usage();
                        return;
                    }
            }

            if(StringUtils.isNoneEmpty(params.getDestinationTable())){
                tableDescription.setTableName(params.getDestinationTable());
            }

            // initialize DynamoDB client and set the endpoint properly
            AmazonDynamoDBClient destination = new AmazonDynamoDBClient(credentialsProvider);
            destination.setEndpoint(params.getDestinationEndpoint());
            DynamoDB destDB = new DynamoDB(destination);
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(params.getDestinationTable())
                    .withKeySchema(tableDescription.getKeySchema())
                    .withAttributeDefinitions(tableDescription.getAttributeDefinitions())
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(tableDescription.getProvisionedThroughput().getReadCapacityUnits())
                            .withWriteCapacityUnits(tableDescription.getProvisionedThroughput().getWriteCapacityUnits()));

            Table destTable=destDB.createTable(request);
            destTable.waitForActive();

            LOGGER.info("Table Created Sucessfully");


        }catch (ParameterException e) {
                LOGGER.error(e);
                JCommander.getConsole().println(e.toString());
                cmd.usage();
                System.exit(StatusCodes.EINVAL);
            } catch (Exception e) {
                LOGGER.fatal(e);
                JCommander.getConsole().println(e.toString());;
                System.exit(StatusCodes.EINVAL);
            }

    }



    //read schema from source table

    public static TableDescription getTablefromSourceDB(AWSCredentialsProvider credentialsProvider,CommandLineArgs params){

        // initialize DynamoDB client and set the endpoint properly
        AmazonDynamoDBClient sourceclient = new AmazonDynamoDBClient(credentialsProvider);
        sourceclient.setEndpoint(params.getSourceEndpoint());
        DynamoDB sourceDB = new DynamoDB(sourceclient);
        TableDescription tableDescription =
                sourceDB.getTable(params.getSourceTable()).describe();

        return  tableDescription;

    }

    //read table detail from description
 public static TableDescription  getTableFromSchema(String schemapath) throws FileNotFoundException {
     GsonBuilder gsonBuilder = new GsonBuilder();
    // gsonBuilder.registerTypeAdapter(TableStatus.class, new AttributeScopeDeserializer());
     gsonBuilder.registerTypeAdapter(Date.class, new DateConverter());
     Gson gson = gsonBuilder.create();
     TableDescription element = gson.fromJson(new FileReader(schemapath), TableDescription.class);
     return element;

 }




}



class DateConverter implements JsonDeserializer<Date>{
    @Override
    public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        return new Date(json.getAsLong());
    }
}

/*
class AttributeScopeDeserializer implements JsonDeserializer<TableStatus>
{
    @Override
    public TableStatus deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException
    {
        TableStatus[] scopes = TableStatus.values();
        for (TableStatus scope : scopes)
        {
            if (scope.name().equals(json.getAsString()))
                return scope;
        }
        return null;
    }

}
*/