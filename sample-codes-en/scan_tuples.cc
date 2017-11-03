#include <NdbApi.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *connectstring = "127.0.0.1:1186";
const char *db = "world";

int print_error(const NdbError &e, const char *msg)
{
  fprintf(stderr, "%s: Error code (%d): %s.\n",
          msg, e.code, e.message);
}

// 1. 関数の定義
int do_scan_read(Ndb *ndb);
int do_index_scan_read(Ndb *ndb);
int do_scan_update(Ndb *ndb);

// 2. 行末の空白を取り除く
void trim(char *str, size_t size) {
  char *end = str + size;
  char *saved_pos = NULL;
  while (str != end) {
    if (*str == ' ' || *str == '\t') {
      if (saved_pos == NULL)
        saved_pos = str;
    } else {
      saved_pos = NULL;
    }
    str++;
  }

  if (saved_pos) {
    *saved_pos = '\0';
  }
}

int main(int argc, char** argv)
{
  ndb_init();
  Ndb_cluster_connection *cluster_connection = new Ndb_cluster_connection(connectstring);
  if (cluster_connection->connect(4 /* retries               */,
                                  5 /* delay between retries */,
                                  1 /* verbose               */)) {
    fprintf(stderr, "Could not connect to MGMD.");
    return 1;
  }

  if (cluster_connection->wait_until_ready(30,0) < 0) {
    fprintf(stderr, "Could not connect to NDBD.");
    return 2;
  }

  Ndb *myNdb = new Ndb(cluster_connection, db);
  if (myNdb->init()) {
    print_error(myNdb->getNdbError(),
                "Could not connect to the database object.");
    return 3;
  }

  // 関数の呼び出し
  int err = 0;
  if ((err = do_scan_read(myNdb)) |
      (err = do_index_scan_read(myNdb)) |
      (err = do_scan_update(myNdb))) {
    printf("Transaction failed due to error (%d)\n.", err);
  }

  delete myNdb;
  delete cluster_connection;
  ndb_end(0);
  return 0;
}

int do_scan_read(Ndb *ndb)
{
  const NdbDictionary::Dictionary* myDict = ndb->getDictionary();
  const NdbDictionary::Table *myTable = NULL;
  const NdbDictionary::Column *myColumn = NULL;

  // 3. メタデータの取得。フィルタで使用するため、NdbDictionary::Columnクラスの
  //    インスタンスを取得している点に注意。
  if ((myTable = myDict->getTable("City")) == NULL ||
      (myColumn = myTable->getColumn("CountryCode")) == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve matadata.");
    return 1;
  }

  NdbTransaction *myTransaction = ndb->startTransaction();
  if (myTransaction == NULL) {
    print_error(ndb->getNdbError(), "Could not start transaction.");
    return 2;
  }

  // 4. スキャンオペレーションオブジェクトの取得
  NdbScanOperation *sop = myTransaction->getNdbScanOperation(myTable);
  if (sop == NULL) {
    print_error(myTransaction->getNdbError(),
                "Could not retrieve an operation.");
    ndb->closeTransaction(myTransaction);
    return 3;
  }

  // 5. カーソルのスクロールを指示。読み取りモードはREAD COMMITED
  Uint32 scanFlags = NdbScanOperation::SF_TupScan;
  sop->readTuples(NdbOperation::LM_CommittedRead, scanFlags);

  // 6. フィルタの設定
  NdbScanFilter filter(sop);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
     filter.cmp(NdbScanFilter::COND_LIKE, myColumn->getColumnNo(), "JPN", 3) < 0 ||
     filter.end() < 0) {
    print_error(myTransaction->getNdbError(), "Failed to set a filter.");
    ndb->closeTransaction(myTransaction);
    return 4;
  }

  // 7. 読み取った値を格納するバッファ
  NdbRecAttr *id = sop->getValue("ID");
  NdbRecAttr *name = sop->getValue("Name");
  NdbRecAttr *cc = sop->getValue("CountryCode");
  NdbRecAttr *population = sop->getValue("Population");

  // 8. スキャンの指示を送信
  if (myTransaction->execute( NdbTransaction::NoCommit ) == -1) {
    print_error(myTransaction->getNdbError(), "Failed to prepare a scan.");
    ndb->closeTransaction(myTransaction);
    return 5;
  }

  // 9. メインループ
  int check = 0;
  while((check = sop->nextResult(true)) == 0) {
    do {
      char name_buf[36], cc_buf[4];
      bzero(name_buf, sizeof(name_buf));
      bzero(cc_buf, sizeof(cc_buf));
      memcpy(name_buf, name->aRef(), name->get_size_in_bytes());
      trim(name_buf, name->get_size_in_bytes());
      memcpy(cc_buf, cc->aRef(), cc->get_size_in_bytes());
      // trim(cc_buf, cc->get_size_in_bytes()); // 常に3文字のため不要
      cc_buf[cc->get_size_in_bytes()] = 0;
      printf("ID: %d, Name: %s, Country: %s, Populatoin: %d\n",
             id->u_32_value(),
             name_buf,
             cc_buf,
             population->u_32_value());
    } while((check = sop->nextResult(false)) == 0);

    if (check != -1) {
      check = myTransaction->execute(NdbTransaction::NoCommit);   
    }
  }
  if (check != -1) {
    check = myTransaction->execute(NdbTransaction::Commit);
  }

  // トランザクションの破棄
  ndb->closeTransaction(myTransaction);

  return check != 0 ? 6 : 0;
}

int do_index_scan_read(Ndb *ndb)
{
  const NdbDictionary::Dictionary* myDict = ndb->getDictionary();
  const NdbDictionary::Index *myIndex = NULL;
  const NdbDictionary::Table *myTable = NULL;
  const NdbDictionary::Column *myColumn = NULL;

  // 10. インデックススキャンの対象となるインデックス（Population）のメタデータを取得
  if ((myIndex = myDict->getIndex("Population", "City")) == NULL ||
      (myTable = myDict->getTable("City")) == NULL ||
      (myColumn = myTable->getColumn("CountryCode")) == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve matadata.");
    return 1;
  }
  
  NdbTransaction *myTransaction = ndb->startTransaction();
  if (myTransaction == NULL) {
    print_error(ndb->getNdbError(), "Could not start transaction.");
    return 2;
  }

  // 11. インデックススキャンオペレーションオブジェクトの取得
  NdbIndexScanOperation *isop = myTransaction->getNdbIndexScanOperation(myIndex);
  if (isop == NULL) {
    print_error(myTransaction->getNdbError(), "Could not retrieve an operation.");
    ndb->closeTransaction(myTransaction);
    return 3;
  }

  // 12. インデックスを使って降順にソート
  Uint32 scanFlags = NdbScanOperation::SF_Descending;
  isop->readTuples(NdbOperation::LM_CommittedRead, scanFlags);

  // 13. インデックスの検索範囲を指定
  Uint32 low = 1000000;
  if (isop->setBound("Population", NdbIndexScanOperation::BoundLE, (char*)&low)) {
    print_error(myTransaction->getNdbError(), "Could not retrieve an operation.");
    ndb->closeTransaction(myTransaction);
    return 4;
  }

  if (isop->end_of_bound(0)) {
    print_error(myTransaction->getNdbError(), "Could not retrieve an operation.");
    ndb->closeTransaction(myTransaction);
    return 5;
  }

  NdbScanFilter filter(isop);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
     filter.cmp(NdbScanFilter::COND_LIKE, myColumn->getColumnNo(), "JPN", 3) < 0 ||
     filter.end() < 0) {
    print_error(myTransaction->getNdbError(), "Failed to set a filter.");
    ndb->closeTransaction(myTransaction);
    return 6;
  }

  NdbRecAttr *id = isop->getValue("ID");
  NdbRecAttr *name = isop->getValue("Name");
  NdbRecAttr *cc = isop->getValue("CountryCode");
  NdbRecAttr *population = isop->getValue("Population");

  if (myTransaction->execute( NdbTransaction::NoCommit ) == -1) {
    print_error(myTransaction->getNdbError(), "Failed to prepare a scan.");
    ndb->closeTransaction(myTransaction);
    return 7;
  }

  int check = 0;
  while((check = isop->nextResult(true)) == 0) {
    do {
      char name_buf[36], cc_buf[4];
      bzero(name_buf, sizeof(name_buf));
      bzero(cc_buf, sizeof(cc_buf));
      memcpy(name_buf, name->aRef(), name->get_size_in_bytes());
      trim(name_buf, name->get_size_in_bytes());
      memcpy(cc_buf, cc->aRef(), cc->get_size_in_bytes());
      // trim(cc_buf, cc->get_size_in_bytes());
      cc_buf[cc->get_size_in_bytes()] = 0;
      printf("ID: %d, Name: %s, Country: %s, Populatoin: %d\n",
             id->u_32_value(),
             name_buf,
             cc_buf,
             population->u_32_value());
    } while((check = isop->nextResult(false)) == 0);

    if (check != -1) {
      check = myTransaction->execute(NdbTransaction::NoCommit);
    }
  }
  if (check != -1) {
    check = myTransaction->execute(NdbTransaction::Commit);   
  }

  ndb->closeTransaction(myTransaction);

  return check != 0 ? 8 : 0;
}

int do_scan_update(Ndb *ndb)
{
  const NdbDictionary::Dictionary* myDict = ndb->getDictionary();
  const NdbDictionary::Table *myTable = NULL;
  const NdbDictionary::Column *myColumn = NULL;
    
  if ((myTable = myDict->getTable("City")) == NULL ||
      (myColumn = myTable->getColumn("CountryCode")) == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve matadata.");
    return 1;
  }

  NdbTransaction *myTransaction = ndb->startTransaction();
  if (myTransaction == NULL) {
    print_error(ndb->getNdbError(), "Could not start transaction.");
    return 2;
  }

  NdbScanOperation *sop = myTransaction->getNdbScanOperation(myTable);
  if (sop == NULL) {
    print_error(myTransaction->getNdbError(), "Could not retrieve an operation.");
    ndb->closeTransaction(myTransaction);
    return 3;
  }

  // 14. 更新処理のため排他ロックを指定
  sop->readTuples(NdbOperation::LM_Exclusive, 0);

  NdbScanFilter filter(sop);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
     filter.cmp(NdbScanFilter::COND_LIKE, myColumn->getColumnNo(), "JPN", 3) < 0 ||
     filter.end() < 0) {
    print_error(myTransaction->getNdbError(), "Failed to set a filter");
    ndb->closeTransaction(myTransaction);
    return 4;
  }

  if(myTransaction->execute( NdbTransaction::NoCommit ) == -1) {
    print_error(myTransaction->getNdbError(), "Could not prepare a scan");
    ndb->closeTransaction(myTransaction);
    return 5;
  }

  int check = 0;
  while((check = sop->nextResult(true)) == 0) {
    do {
      // 15. 更新用のオペレーションオブジェクトを取得
      NdbOperation *uop = sop->updateCurrentTuple();
      if (uop == NULL) {
        print_error(myTransaction->getNdbError(), "Cannot retrieve an update operation");
        ndb->closeTransaction(myTransaction);
        return 6;
      }
      uop->setValue("CountryCode", "ZPG");
    } while((check = sop->nextResult(false)) == 0);

    if (check != -1) {
      check = myTransaction->execute(NdbTransaction::NoCommit);
    }
  }
  check = myTransaction->execute(NdbTransaction::Commit);
  ndb->closeTransaction(myTransaction);
  return check != 0 ? 9 : 0;
}
