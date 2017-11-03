#include <NdbApi.hpp>
#include <iostream>
#include <string>

const char *connectstring = "127.0.0.1:1186";
const char *db = "world";

int print_error(const NdbError &e, const char *msg)
{
  std::cerr << msg << ": Error code (" << e.code << "): "
            << e.message << "." << std::endl;
}

class ReadTupleByAttr {
  
}

int main(int argc, char** argv)
{
  // Step 1. Initialize NDB API
  ndb_init();

  // Step 2. Connecting to the cluster
  Ndb_cluster_connection *cluster_connection =
    new Ndb_cluster_connection(connectstring);
  if (cluster_connection->connect(4 /* retries               */,
                                  5 /* delay between retries */,
                                  1 /* verbose               */)) {
    std::cerr << "Could not connect to MGMD." << std::endl;
    return 1;
  }

  if (cluster_connection->wait_until_ready(30,0) < 0) {
    std::cerr << "Could not connect to NDBD." << std::endl;
    return 2;
  }

  // Step 3. Connect to 'world' database
  Ndb *myNdb = new Ndb(cluster_connection, db);
  if (myNdb->init()) {
    print_error(myNdb->getNdbError(),
                "Could not connect to the database object.");
    return 3;
  }

  // Step 4. Get table metadata
  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("Country");
    
  if (myTable == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve a table.");
    return 4;
  }
  
  // Step 5. Start transaction
  NdbTransaction *myTransaction= myNdb->startTransaction();
  if (myTransaction == NULL) {
    print_error(myNdb->getNdbError(), "Could not start transaction.");
    return 5;
  }
  
  // Step 6. Get operation handle
  NdbOperation *myOperation= myTransaction->getNdbOperation(myTable);
  if (myOperation == NULL) {
    print_error(myTransaction->getNdbError(),
                "Could not retrieve an operation.");
    return 6;
  }
  
  // Step 7. Specify type of operation and search condition
  myOperation->readTuple(NdbOperation::LM_Read);
  myOperation->equal("Code", "JPN");
    
  // Step 8. Get buffers for results
  NdbRecAttr *Name = myOperation->getValue("Name", NULL);
  NdbRecAttr *Capital = myOperation->getValue("Capital", NULL);
  if (Name == NULL || Capital == NULL) {
    print_error(myTransaction->getNdbError(),
                "Could not allocate attribute records.");
    return 7;
  }

  // Step 9. Send a request to data nodes
  if (myTransaction->execute( NdbTransaction::Commit ) == -1) {
    print_error(myTransaction->getNdbError(), "Transaction failed.");
    return 8;
  }

  // Step 10. Retrieve values
  std::cout << " Name:         "
            << std::string(Name->aRef(),
                           Name->get_size_in_bytes())
            << std::endl;
  std::cout << " Capital Code: " << Capital->u_32_value() << std::endl;

cleanup:
  // Step 11. Cleanup
  if (myTransaction) myNdb->closeTransaction(myTransaction);
  if (myNdb) delete myNdb;
  if (cluster_connection) delete cluster_connection;
  ndb_end(0);
  return 0;
}
