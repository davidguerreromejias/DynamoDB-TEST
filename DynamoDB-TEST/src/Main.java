import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.KeyType;

public class Main {
	
	public static AmazonDynamoDB dynamoDB;

	public static void main(String[] args) {

		dynamoDB= AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.EU_WEST_1)
				.build();
		
		listMyTables();
		Table table = getTable("realdemand_S3metadata");
		tableInformation(table);
		scanTable(table);
		createItem(table);
		deleteItem(table);
		createTable();
	}
	
    private static void listMyTables() {

    	List<String> tables = dynamoDB.listTables().getTableNames();

        System.out.println("Listing table names:");

        for(int i = 0; i < tables.size(); i++) {
            System.out.println(tables.get(i));
        }
    }

    private static Table getTable(String tableName) {

    	DynamoDB DB = new DynamoDB(dynamoDB);
		Table firstTable = DB.getTable(tableName);
        return firstTable;
        
    }
    
    private static void tableInformation(Table table) {
        System.out.println("Describing " + table.getTableName());

        TableDescription tableDescription = table.describe();
        System.out.format(
            "Name: %s:\n" + "Status: %s \n" + "Provisioned Throughput (read capacity units/sec): %d \n"
                + "Provisioned Throughput (write capacity units/sec): %d \n",
            tableDescription.getTableName(), tableDescription.getTableStatus(),
            tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
            tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
    }
    
    private static void scanTable(Table table) {
    	//Item item = table.getItem("is_processed-date_generated", "1_login");
    	Item item = table.getItem("object_path","0-raw/realdemand/loginJson/20180226/loginJson_201802260756_201802260815_7495bc42c0653774ab51744c21b4e576.jsonl");
    	System.out.println(item.toJSONPretty());
    	
    	Index index = table.getIndex("is_processed-date_generated");
    	QuerySpec spec = new QuerySpec()
    		    .withKeyConditionExpression("is_processed = :v_processed and date_generated = :v_date")
    		    .withValueMap(new ValueMap()
    		        .withString(":v_processed","1_login")
    		        .withString(":v_date","2018-02-26T08:15:00+0000"));
    	
    	ItemCollection<QueryOutcome> items = index.query(spec);
    	Iterator<Item> iter = items.iterator(); 
    	while (iter.hasNext()) {
    	    System.out.println(iter.next().toJSONPretty());
    	}
    }
    
    private static void createItem(Table table) {
    	
    	Item item = new Item()
    	    .withPrimaryKey("author", "david")
    	    .withString("bucket_name", "vueling-data-lake")
    	    .withString("date_generated", "2018-02-26T08:15:00+0000")
    	    .withString("date_processed", "2018-02-26T08:15:00+0000")
    	    .withString("date_uploaded", "2018-02-26T08:15:00+0000")
    	    .withString("file_format", "jsonl")
    	    .withNumber("file_size", 31303857)
    	    .withString("file_type", "login")
    	    .withNumber("has_error", 0)
    	    .withString("is_processed", "1_login")
    	    .withString("layer", "raw")
    	    .withString("object_path", "david.json")
    	    .withNumber("row_count", 96600);

    	PutItemOutcome outcome = table.putItem(item);
    	System.out.println("Item added to table "+ table.getTableName());
    }
    
    private static void deleteItem(Table table) {
    	table.deleteItem("object_path", "david.json");
    	System.out.println("Item deleted from table "+ table.getTableName());
    }
    
    private static void createTable() {

    	List<KeySchemaElement> elements = new ArrayList<KeySchemaElement>();
    	
        KeySchemaElement keySchemaElement = new KeySchemaElement()
                .withKeyType(KeyType.HASH)
                .withAttributeName("email");
        elements.add(keySchemaElement);
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("email")
                .withAttributeType(ScalarAttributeType.S));
        
    	CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName("davidTest")
                .withKeySchema(elements)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L))
                .withAttributeDefinitions(attributeDefinitions);
    	
    	System.out.println("Table created with name "+ createTableRequest.getTableName());
    }

}
