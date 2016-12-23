package com.amazonaws.service.dynamodbv2.backup;


import com.beust.jcommander.Parameter;

public class CommandLineArgs {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;


    public boolean getHelp() {
        return help;
    }

    public static final String TYPE = "--type";
    @Parameter(names = TYPE, description = "mention import/export")
    public String type;

    public String getType() {
        return type;
    }

    public static final String SCHEMA_JSON = "--schema";
    @Parameter(names = SCHEMA_JSON, description = "DynamoDB schema in Json format")
    private String schemaJson;

    public String getSchemaJson() {
        return schemaJson;
    }


    public static final String SOURCE_ENDPOINT = "--sourceEndpoint";
    @Parameter(names = SOURCE_ENDPOINT, description = "DynamoDB endpoint of the source table")
    private String sourceEndpoint;

    public String getSourceEndpoint() {
        return sourceEndpoint;
    }

    public static final String SOURCE_TABLE = "--sourceTable";
    @Parameter(names = SOURCE_TABLE, description = "Name of the source table")
    private String sourceTable;

    public String getSourceTable() {
        return sourceTable;
    }

    public static final String DESTINATION_ENDPOINT = "--destinationEndpoint";
    @Parameter(names = DESTINATION_ENDPOINT, description = "DynamoDB endpoint of the destination table", required = true)
    private String destinationEndpoint;

    public String getDestinationEndpoint() {
        return destinationEndpoint;
    }

    public static final String DESTINATION_TABLE = "--destinationTable";
    @Parameter(names = DESTINATION_TABLE, description = "Name of the destination table", required = true)
    private String destinationTable;

    public String getDestinationTable() {
        return destinationTable;
    }

    public static final String PROFILE_NAME = "--profile";
    @Parameter(names = PROFILE_NAME, description = "Name of profile, used to connect Dynamo DB")
    private String profileName;

    public String getProfileName() {
        return profileName;
    }

}
