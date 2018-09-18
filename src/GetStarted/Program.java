package GetStarted;

import java.io.IOException;

import com.microsoft.azure.cosmos.ConnectionPolicy;
import com.microsoft.azure.cosmos.ConsistencyLevel;
import com.microsoft.azure.cosmos.DataType;
import com.microsoft.azure.cosmos.Database;
import com.microsoft.azure.cosmos.Item;
import com.microsoft.azure.cosmos.CosmosClient;
import com.microsoft.azure.cosmos.CosmosClientException;
import com.microsoft.azure.cosmos.Container;
import com.microsoft.azure.cosmos.FeedOptions;
import com.microsoft.azure.cosmos.FeedResponse;
import com.microsoft.azure.cosmos.Index;
import com.microsoft.azure.cosmos.IndexingPolicy;
import com.microsoft.azure.cosmos.RangeIndex;
import com.microsoft.azure.cosmos.RequestOptions;

public class Program {

    private CosmosClient client;

    /**
     * Run a Hello Cosmos DB console application.
     * 
     * @param args
     *            command line arguments
     * @throws CosmosClientException
     *             exception
     * @throws IOException 
     */
    public static void main(String[] args) {

        try {
            Program p = new Program();
            p.getStartedDemo();
            System.out.println(String.format("Demo complete, please hold while resources are deleted"));
        } catch (Exception e) {
            System.out.println(String.format("Cosmos DB GetStarted failed with %s", e));
        }
    }

    private void getStartedDemo() throws CosmosClientException, IOException {
        this.client = new CosmosClient("https://FILLME.documents.azure.com",
                "FILLME", 
                new ConnectionPolicy(),
                ConsistencyLevel.Session);

        this.createDatabaseIfNotExists("FamilyDB");
        this.createContainerIfNotExists("FamilyDB", "FamilyContainer");

        Family andersenFamily = getAndersenFamilyItem();
        this.createFamilyItemIfNotExists("FamilyDB", "FamilyContainer", andersenFamily);

        Family wakefieldFamily = getWakefieldFamilyItem();
        this.createFamilyItemIfNotExists("FamilyDB", "FamilyContainer", wakefieldFamily);

        this.executeSimpleQuery("FamilyDB", "FamilyContainer");

        this.client.deleteDatabase("/dbs/FamilyDB", null);
    }

    private Family getAndersenFamilyItem() {
        Family andersenFamily = new Family();
        andersenFamily.setId("Andersen.1");
        andersenFamily.setLastName("Andersen");

        Parent parent1 = new Parent();
        parent1.setFirstName("Thomas");

        Parent parent2 = new Parent();
        parent2.setFirstName("Mary Kay");

        andersenFamily.setParents(new Parent[] { parent1, parent2 });

        Child child1 = new Child();
        child1.setFirstName("Henriette Thaulow");
        child1.setGender("female");
        child1.setGrade(5);

        Pet pet1 = new Pet();
        pet1.setGivenName("Fluffy");

        child1.setPets(new Pet[] { pet1 });

        andersenFamily.setDistrict("WA5");
        Address address = new Address();
        address.setCity("Seattle");
        address.setCounty("King");
        address.setState("WA");

        andersenFamily.setAddress(address);
        andersenFamily.setRegistered(true);

        return andersenFamily;
    }

    private Family getWakefieldFamilyItem() {
        Family wakefieldFamily = new Family();
        wakefieldFamily.setId("Wakefield.7");
        wakefieldFamily.setLastName("Wakefield");

        Parent parent1 = new Parent();
        parent1.setFamilyName("Wakefield");
        parent1.setFirstName("Robin");

        Parent parent2 = new Parent();
        parent2.setFamilyName("Miller");
        parent2.setFirstName("Ben");

        wakefieldFamily.setParents(new Parent[] { parent1, parent2 });

        Child child1 = new Child();
        child1.setFirstName("Jesse");
        child1.setFamilyName("Merriam");
        child1.setGrade(8);

        Pet pet1 = new Pet();
        pet1.setGivenName("Goofy");

        Pet pet2 = new Pet();
        pet2.setGivenName("Shadow");

        child1.setPets(new Pet[] { pet1, pet2 });

        Child child2 = new Child();
        child2.setFirstName("Lisa");
        child2.setFamilyName("Miller");
        child2.setGrade(1);
        child2.setGender("female");

        wakefieldFamily.setChildren(new Child[] { child1, child2 });

        Address address = new Address();
        address.setCity("NY");
        address.setCounty("Manhattan");
        address.setState("NY");

        wakefieldFamily.setAddress(address);
        wakefieldFamily.setDistrict("NY23");
        wakefieldFamily.setRegistered(true);
        return wakefieldFamily;
    }

    private void createDatabaseIfNotExists(String databaseName) throws CosmosClientException, IOException {
        String databaseLink = String.format("/dbs/%s", databaseName);

        // Check to verify a database with the id=FamilyDB does not exist
        try {
            this.client.readDatabase(databaseLink, null);
            this.writeToConsoleAndPromptToContinue(String.format("Found %s", databaseName));
        } catch (CosmosClientException de) {
            // If the database does not exist, create a new database
            if (de.getStatusCode() == 404) {
                Database database = new Database();
                database.setId(databaseName);
                
                this.client.createDatabase(database, null);
                this.writeToConsoleAndPromptToContinue(String.format("Created %s", databaseName));
            } else {
                throw de;
            }
        }
    }

    private void createContainerIfNotExists(String databaseName, String containerName) throws IOException,
    CosmosClientException {
        String databaseLink = String.format("/dbs/%s", databaseName);
        String containerLink = String.format("/dbs/%s/colls/%s", databaseName, containerName);

        try {
            this.client.readContainer(containerLink, null);
            writeToConsoleAndPromptToContinue(String.format("Found %s", containerName));
        } catch (CosmosClientException de) {
            // If the container does not exist, create a new container
            if (de.getStatusCode() == 404) {
                Container containerInfo = new Container();
                containerInfo.setId(containerName);

                // Optionally, you can configure the indexing policy of a
                // container. Here we configure container for maximum query
                // flexibility including string range queries.
                RangeIndex index = new RangeIndex(DataType.String);
                index.setPrecision(-1);

                containerInfo.setIndexingPolicy(new IndexingPolicy(new Index[] { index }));

                // Cosmos DB container can be reserved with throughput
                // specified in request units/second. 1 RU is a normalized
                // request equivalent to the read of a 1KB document. Here we
                // create a container with 400 RU/s.
                RequestOptions requestOptions = new RequestOptions();
                requestOptions.setOfferThroughput(400);

                this.client.createContainer(databaseLink, containerInfo, requestOptions);

                this.writeToConsoleAndPromptToContinue(String.format("Created %s", containerName));
            } else {
                throw de;
            }
        }

    }

    private void createFamilyItemIfNotExists(String databaseName, String containerName, Family family)
            throws CosmosClientException, IOException {
        try {
            String itemLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, containerName, family.getId());
            this.client.readItem(itemLink, new RequestOptions());
        } catch (CosmosClientException de) {
            if (de.getStatusCode() == 404) {
                String containerLink = String.format("/dbs/%s/colls/%s", databaseName, containerName);
                this.client.createItem(containerLink, family, new RequestOptions(), true);
                this.writeToConsoleAndPromptToContinue(String.format("Created Family %s", family.getId()));
            } else {
                throw de;
            }
        }
    }

    private void executeSimpleQuery(String databaseName, String containerName) {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setPageSize(-1);
        queryOptions.setEnableCrossPartitionQuery(true);

        String containerLink = String.format("/dbs/%s/colls/%s", databaseName, containerName);
        FeedResponse<Item> queryResults = this.client.queryItems(containerLink,
                "SELECT * FROM Family WHERE Family.lastName = 'Andersen'", queryOptions);

        System.out.println("Running SQL query...");
        for (Item family : queryResults.getQueryIterable()) {
            System.out.println(String.format("\tRead %s", family));
        }
    }

    @SuppressWarnings("unused")
    private void replaceFamilyItem(String databaseName, String containerName, String familyName, Family updatedFamily)
            throws IOException, CosmosClientException {
        try {
            this.client.replaceItem(
                    String.format("/dbs/%s/colls/%s/docs/%s", databaseName, containerName, updatedFamily.getId()), updatedFamily,
                    null);
            writeToConsoleAndPromptToContinue(String.format("Replaced Family %s", updatedFamily.getId()));
        } catch (CosmosClientException de) {
            throw de;
        }
    }

    @SuppressWarnings("unused")
    private void deleteFamilyItem(String databaseName, String containerName, String itemName) throws IOException,
            CosmosClientException {
        try {
            this.client.deleteItem(String.format("/dbs/%s/colls/%s/docs/%s", databaseName, containerName, itemName), null);
            writeToConsoleAndPromptToContinue(String.format("Deleted Family %s", itemName));
        } catch (CosmosClientException de) {
            throw de;
        }
    }

    private void writeToConsoleAndPromptToContinue(String text) throws IOException {
        System.out.println(text);
        System.out.println("Press any key to continue ...");
        System.in.read();
    }
}

