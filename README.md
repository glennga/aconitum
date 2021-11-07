# aconitum
This repository contains scripts to test various TPC-CH queries across AsterixDB, MongoDB, and Couchbase. The goal of this repository is to evaluate how well each system utilizes their multi-valued indexes for use in real-world analytical queries.

## Experimental Setup
All experiments have been executed on AWS EC2 instances. Each node was of type `c5.xlarge` (4 CPUs @ 3.4GHz, 8GB of RAM) with 3000 IOPS SSDs attached. Each node was running Ubuntu Server 20.04 LTS. We record the client response time here, so each experiment executed local to each database server (to minimize client-server communication latencies). 

### TPC CH(2)
A modified TPC-CH was utilized here, one that a) more naturally represents orderlines within orders as nested documents, and b) has orderline dates that are uniformly distributed across 7 years. The parameters used for TPC-CH generator were `scaleFactor=1` and `numWarehouses=500`.

### All Systems

1. Install `python 3.8`, and `python3-pip`.
2. Clone this repository onto the server node. Ensure you have the correct requirements.

```bash
# Clone the repository.
git clone https://github.com/glennga/aconitum.git

# Install the requirements (a virtual environment also works).
cd aconitum
python3 -m pip install -r requirements.txt
```
3. Ensure that your `python3` searches the `aconitum` repository for modules. Add the following line to your `.bashrc` file.

```bash
export PYTHONPATH=$PYTHONPATH:/home/ubuntu/aconitum
```

### AsterixDB
1. Ensure that AsterixDB is installed with `java 11` and configured on the node to run the experiments on. The cc.conf file used is as follows:
```
[nc/asterix_nc1]
txn.log.dir=txnlog
iodevices=iodevice
core.dump.dir=coredump
nc.api.port=19004
address=localhost

[nc]
command=asterixnc

[cc]
address=localhost

[common]
log.dir=logs/
log.level=INFO
```

2. Create the dataverse required for this experiment. 

```sql
DROP    DATAVERSE TPC_CH IF EXISTS;
CREATE  DATAVERSE TPC_CH;
USE     TPC_CH;

CREATE  TYPE CustomerType AS { c_w_id: bigint, c_d_id: bigint, c_id: bigint };
CREATE  TYPE NationType AS { n_nationkey: bigint };
CREATE  TYPE OrdersType AS { o_w_id: bigint, o_d_id: bigint, o_id: bigint  };
CREATE  TYPE StockType AS { s_w_id: bigint, s_i_id: bigint };
CREATE  TYPE ItemType AS { i_id: bigint };
CREATE  TYPE RegionType AS { r_regionkey: bigint };
CREATE  TYPE SupplierType AS { su_suppkey: bigint };

CREATE  DATASET Customer (CustomerType) PRIMARY KEY c_w_id, c_d_id, c_id;
CREATE  DATASET Nation (NationType) PRIMARY KEY n_nationkey;
CREATE  DATASET Orders (OrdersType) PRIMARY KEY o_w_id, o_d_id, o_id;
CREATE  DATASET Stock (StockType) PRIMARY KEY s_w_id, s_i_id;
CREATE  DATASET Item (ItemType) PRIMARY KEY i_id;
CREATE  DATASET Region (RegionType) PRIMARY KEY r_regionkey;
CREATE  DATASET Supplier (SupplierType) PRIMARY KEY su_suppkey; 
```

3. Load each dataset in the dataverse. Adjust the path accordingly.

```sql
LOAD DATASET TPC_CH.Customer USING localfs (
  ("path"="localhost:///home/ubuntu/aconitum/resources/tpc_ch/customer.json"), 
  ("format"="json") 
);
LOAD DATASET TPC_CH.Nation USING localfs ( 
  ("path"="localhost:///home/ubuntu/aconitum/resources/tpc_ch/nation.json"),
  ("format"="json") 
);
LOAD DATASET TPC_CH.Orders USING localfs ( 
  ("path"="localhost:///home/ubuntu/aconitum/resources/tpc_ch/orders.json"), 
  ("format"="json") 
);
LOAD DATASET TPC_CH.Stock USING localfs ( 
  ("path"="localhost:///home/ubuntu/aconitum/resources/tpc_ch/stock.json"), 
  ("format"="json") 
);    
LOAD DATASET TPC_CH.Item USING localfs ( 
  ("path"="localhost:///home/ubuntu/aconitum/resources/tpc_ch/item.json"), 
  ("format"="json") 
);            
LOAD DATASET TPC_CH.Region USING localfs ( 
  ("path"="localhost:///home/ubuntu/aconitum/resources/tpc_ch/region.json"), 
  ("format"="json") 
);                  
LOAD DATASET TPC_CH.Supplier USING localfs ( 
  ("path"="localhost:///home/ubuntu/aconitum/resources/tpc_ch/supplier.json"), 
  ("format"="json") 
); 
```

4. Build our secondary indexes. This is a separate step to speed-up the previous loading phase.

```sql
USE     TPC_CH;
CREATE  INDEX orderlineDelivDateIdx 
ON      Orders ( 
  UNNEST  o_orderline 
  SELECT  ol_delivery_d : string
) EXCLUDE UNKNOWN KEY;
CREATE  INDEX orderlineItemIdx
ON      Orders (
  UNNEST  o_orderline
  SELECT  ol_i_id : bigint
) EXCLUDE UNKNOWN KEY;
```

5. Execute the benchmark query suite for AsterixDB.

```bash
python3 aconitum/_asterixdb.py
```

6. Analyze the results! The results will be stored in the `out` folder under `results.json` as single line JSON documents.

### Couchbase

1. Ensure that Couchbase 7.0 is installed and configured on the node to run the experiments on. Docs on the install can be found [here](https://docs.couchbase.com/server/7.0/install/ubuntu-debian-install.html). Use the deb package instructions to get the latest and greatest. The memory for the Data service is half of the system memory (4096 MB). The only other service that is made available is the Index service (2048 MB). The `Max Parallelism` parameter should be set to the number of cores of your system (in our case, 4).
2. Create the bucket to hold all of your data. This should utilize the entire cluster's memory-data quota (4096 MB in this case). Change the bucket's ejection-method policy from _Value-only_ to _Full_.
3. Create the collections. The default scope of the bucket houses each collection.

```sql
CREATE  COLLECTION aconitum._default.Customer;
CREATE  COLLECTION aconitum._default.Nation;
CREATE  COLLECTION aconitum._default.Orders;
CREATE  COLLECTION aconitum._default.Stock;
CREATE  COLLECTION aconitum._default.Item;
CREATE  COLLECTION aconitum._default.Region;
CREATE  COLLECTION aconitum._default.Supplier;
```

4. Load each collection in the bucket. Adjust the path accordingly.

```bash
for c in customer nation orders stock item region supplier; do
  /opt/couchbase/bin/cbimport json \
    --cluster localhost --username "admin" --password "password" \
    --bucket "aconitum" --scope-collection-exp _default.${c^} \
    --dataset file:///home/ubuntu/aconitum/resources/tpc_ch/$c.json \
    --format lines --generate-key key::#UUID#
done
```

5. Build the indexes associated with each collection.

```sql
CREATE  INDEX customerPrimaryKeyIdx 
ON      aconitum._default.Customer ( c_w_id, c_d_id, c_id );
CREATE  INDEX nationPrimaryKeyIdx 
ON      aconitum._default.Nation ( n_nationkey );
CREATE  INDEX ordersPrimaryKeyIdx
ON      aconitum._default.Orders ( o_w_id, o_d_id, o_id );
CREATE  INDEX stockPrimaryKeyIdx
ON      aconitum._default.Stock ( s_w_id, s_i_id );
CREATE  INDEX itemPrimaryKeyIdx
ON      aconitum._default.Item ( i_id );
CREATE  INDEX regionPrimaryKeyIdx
ON      aconitum._default.Region ( r_regionkey );
CREATE  INDEX supplierPrimaryKeyIdx
ON      aconitum._default.Supplier ( su_suppkey );

CREATE  INDEX orderlineDelivDateIdx 
ON      aconitum._default.Orders (
  DISTINCT  ARRAY OL.ol_delivery_d
  FOR       OL 
  IN        o_orderline 
  END
);
CREATE  INDEX orderlineItemIdx
ON      aconitum._default.Orders (
  DISTINCT  ARRAY OL.ol_i_id
  FOR       OL 
  IN        o_orderline 
  END
);
CREATE  PRIMARY INDEX ordersPrimaryIdx
ON      aconitum._default.Orders;
CREATE  PRIMARY INDEX itemPrimaryIdx 
ON      aconitum._default.Item;
```

6. Execute the benchmark query suite for Couchbase.

```bash
python3 aconitum/_couchbase.py
```

7. Analyze the results! The results will be stored in the `out` folder under `results.json` as single line JSON documents.


### MongoDB

1. Ensure that MongoDB is installed and configured on the node to run the experiments on. Docs on the install can be found [here](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/). Enable access control for a user.

```javascript
use admin

db.createUser ({
  user: "admin",
  pwd: "password",
  roles: [ "root" ]
})
```

2. Create the database and collections required for this experiment.

```javascript
use aconitum

db.createCollection ( "Customer" )
db.createCollection ( "Nation" )
db.createCollection ( "Orders" )
db.createCollection ( "Stock" )
db.createCollection ( "Item" )
db.createCollection ( "Region" )
db.createCollection ( "Supplier" )
```

3. Load each collection in the database. Adjust the path accordingly.

```bash
for c in customer nation orders stock item region supplier; do
  mongoimport \
    --authenticationDatabase admin \
    --db aconitum \
    --collection ${c^} \
    --host localhost:27017 \
    --username admin \
    --password password \
    --drop \
    /home/ubuntu/aconitum/resources/tpc_ch/$c.json
done
```

4. Create the indexes for this experiment.

```javascript
use aconitum

db.Customer.createIndex (
  { "c_w_id": 1, "c_d_id": 1, "c_id": 1 },
  { name: "customerPrimaryKeyIdx" }
)
db.Nation.createIndex (
  { "n_nationkey": 1 },
  { name: "nationPrimaryKeyIdx" }
)
db.Orders.createIndex (
  { "o_w_id": 1, "o_d_id": 1, "o_id": 1 },
  { name: "ordersPrimaryKeyIdx" }
)
db.Stock.createIndex (
  { "s_w_id": 1, "s_i_id": 1 },
  { name: "stockPrimaryKeyIdx" }
)
db.Item.createIndex (
  { "i_id": 1 },
  { name: "itemPrimaryKeyIdx" }
)
db.Region.createIndex (
  { "r_regionkey": 1 },
  { name: "regionPrimaryKeyIdx" }
)
db.Supplier.createIndex (
  { "su_suppkey": 1 },
  { name: "supplierPrimaryKeyIdx" }
)

db.Orders.createIndex (
  { "o_orderline.ol_delivery_d": 1 }, 
  { name: "orderlineDelivDateIdx" } 
)
db.Orders.createIndex (
  { "o_orderline.ol_i_id": 1 },
  { name: "orderlineItemIdx" }
)
```

5. Execute the benchmark query suite for MongoDB.

```bash
python3 aconitum/_mongodb.py
```

6. Analyze the results! The results will be stored in the `out` folder under `results.json` as single line JSON documents.