# aconitum
This repository contains scripts to test various TPC-CH queries across AsterixDB, MongoDB, and Couchbase. The goal of this repository is to evaluate how well each system utilizes their multi-valued indexes for use in real-world analytical queries.

## Experimental Setup
All experiments have been executed on AWS EC2 instances. Each node was of type `c5.xlarge` (4 CPUs @ 3.4GHz, 8GB of RAM) with 3000 IOPS SSDs attached. Each node was running Ubuntu Server 20.04 LTS. We record the client response time here, so each experiment executed local to each database server (to minimize client-server communication latencies). 

### TPC CH(2)
A modified TPC-CH was utilized here, one that a) more naturally represents orderlines within orders as nested documents, and b) has orderline dates that are uniformly distributed across 7 years. The parameters used for TPC-CH generator were `scaleFactor=1` and `numWarehouses=200`.

### AsterixDB
1. Ensure that AsterixDB is installed and configured on the node to run the experiments on. Install `java 11` , `python 3.8`, and `python3-pip`. The cc.conf file used is as follows:
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

2. Create the dataverse (`TPC_CH`) required for this experiment.
```sql
DROP DATAVERSE TPC_CH IF EXISTS;
CREATE DATAVERSE TPC_CH;
USE TPC_CH;

CREATE TYPE CustomerType AS { c_w_id: bigint, c_d_id: bigint, c_id: bigint };
CREATE DATASET Customer (CustomerType) PRIMARY KEY c_w_id, c_d_id, c_id;

CREATE TYPE NationType AS { n_nationkey: bigint };
CREATE DATASET Nation (NationType) PRIMARY KEY n_nationkey;

CREATE TYPE OrdersType AS { o_w_id: bigint, o_d_id: bigint, o_id: bigint  };
CREATE DATASET Orders (OrdersType) PRIMARY KEY o_w_id, o_d_id, o_id;

CREATE TYPE StockType AS { s_w_id: bigint, s_i_id: bigint };
CREATE DATASET Stock (StockType) PRIMARY KEY s_w_id, s_i_id;

CREATE TYPE ItemType AS { i_id: bigint };
CREATE DATASET Item (ItemType) PRIMARY KEY i_id;

CREATE TYPE RegionType AS { r_regionkey: bigint };
CREATE DATASET Region (RegionType) PRIMARY KEY r_regionkey;

CREATE TYPE SupplierType AS { su_suppkey: bigint };
CREATE DATASET Supplier (SupplierType) PRIMARY KEY su_suppkey; 

CREATE INDEX orderlineDelivDateIdx ON Orders ( 
  UNNEST o_orderline SELECT ol_delivery_d : string 
);
CREATE INDEX orderlineItemIdx ON Orders ( 
  UNNEST o_orderline SELECT ol_i_id : bigint 
);
```

3. Load each dataset in the dataverse (`TPC_CH`). Adjust the path accordingly.

```sql
LOAD DATASET TPC_CH.Customer USING localfs (
  ("path"="localhost:///home/$USER/tpc_ch/customer.json"), 
  ("format"="json") 
);
LOAD DATASET TPC_CH.Nation USING localfs ( 
  ("path"="localhost:///home/$USER/tpc_ch/nation.json"),
  ("format"="json") 
);
LOAD DATASET TPC_CH.Orders USING localfs ( 
  ("path"="localhost:///home/$USER/tpc_ch/orders.json"), 
  ("format"="json") 
);
LOAD DATASET TPC_CH.Stock USING localfs ( 
  ("path"="localhost:///home/$USER/tpc_ch/stock.json"), 
  ("format"="json") 
);    
LOAD DATASET TPC_CH.Item USING localfs ( 
  ("path"="localhost:///home/$USER/tpc_ch/item.json"), 
  ("format"="json") 
);            
LOAD DATASET TPC_CH.Region USING localfs ( 
  ("path"="localhost:///home/$USER/tpc_ch/region.json"), 
  ("format"="json") 
);                  
LOAD DATASET TPC_CH.Supplier USING localfs ( 
  ("path"="localhost:///home/$USER/tpc_ch/supplier.json"), 
  ("format"="json") 
); 
```

4. Clone this repository onto the server node. Ensure you have the correct requirements.
```bash
# Clone the repository.
git clone https://github.com/glennga/aconitum.git

# Install the requirements (a virtual environment also works).
cd aconitum
python3 -m pip install -r requirements.txt
```

5. Execute the benchmark query suite. Ensure that your `python3` searches the `aconitum` repository for modules.

```bash
export PYTHONPATH=$PYTHONPATH:/home/ubuntu/aconitum

cd aconitum
python3 aconitum/asterixdb.py
```

6. Analyze the results! The results will be stored in the `out` folder under `results.json` as single line JSON documents.

### Couchbase


### MongoDB

