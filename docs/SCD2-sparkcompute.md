# SCD2 plugin

Description
-----------
This plugin integrates new data into an existing SCD2 dataset by joining the incoming data against the existing target. 

Use Case
--------
This plugin is used to integrate the incoming data to the Slowly Changing Dimension Type 2 (SCD2) target tables, and to track the history of the record. 

It supports the following patterns:

**New record integration**
If a new record is received from the input flow (with new natural key), it is added as active, with end date equal to a dummy value (9999-12-31).

**Changed record integration**
If a record is received with at least an update, the previous version is closed, with end date = start date of the new record minus
one second) and the new record is added as active, with end date equal to the dummy value (9999-12-31).

**False delta record cut off**
This capability checks if at least one of the fields of the input record has changed from the previous version already present in
the SCD2 table. If it does it is integrated to the table, otherwise it is discarded. This feature allows this plug-in to be agnostic about the input data loading
(delta or full), since it manages only the changed records and avoids integrating records without variations.

**Late arriving data**
The plugin checks the date of completion of each incoming record in order to add them in the right time interval (start/end date) and
to handle records with the date of completion older than the current active version. For example:
Initial status of the SCD2 dataset:

| SubId | TariffId | Status | Start_Date | End_Date |
|-------|----------|--------|------------|----------|
|1      |TP101     |A       |20190801    | 20190831 |
|1      |TP101     |C       |20190901    | 99991231 |
  
Then the following record is received (Effective_Date older than the Start date of the active record):
| SubId | TariffId | Effective_Date |
|-------|----------|---------------|
|1      |TP102     |20190815       |

The new record it is inserted in the related time interval, changing the previous record end date and next record start date and without creating an active
version.
| SubId | TariffId | Status | Start_Date | End_Date |
|-------|----------|--------|------------|----------|
|1      |TP101     |A       |20190801    | 20190814 |
|1      |TP102     |A       |20190815    | 20190831 |
|1      |TP101     |C       |20190901    | 99991231 |

Usage
-----
This plugin takes two inputs. 
1. *The SCD2 target dataset*: Before connecting this dataset to the SCD2 plugin the end_date field from the dataset needs to be dropped. You can do this using Wrangler. 
2. *The dataset representing new data to be integrated*: This dataset does not contain an end_date, but only contains the effective date.


This plugin can then join the two dataset and integrate the new data into the SCD2 dataset. Its configuration parameters are explained below.

Properties
----------
**Key:** The name of the key field. The key field is used to compare the new records with their previous versions. Usually is set as the natural key of the table. 
This field must be a boolean, int, long, float, double, bytes or string type.

**Start Date Field:** The name of the start date field, which is used as the lower limit of record validity. The grouped records are sorted based on this field. 
This must be a timestamp.

**End Date Field:** The name of the end date field, which is used as upper limit of record validity. The sorted results are iterated to compute the value of this 
field based on the start date.

**Fill In Null:** Fill in null fields from most recent previous records with same id. For example, suppose three records with same key
are processed:

| id | start_date | name | email         |
| -- | ---------- |------|---------------|
| 1  | 0          | John | bcd@abc.com   |
| 1  | 10         | null | abc@abc.com   |
| 1  | 100        | null | null          |

After filling in null, the resulting records will be:

| id | start_date | name | email         |
| -- | ---------- |------|---------------|
| 1  | 0          | John | bcd@abc.com   |
| 1  | 10         | John | abc@abc.com   |
| 1  | 100        | John | abc@abc.com   |

**Deduplicate:** Deduplicate consecutive records that have no changes. This can be used with the blacklist to exclude some fields from comparison.
The blacklist typically includes the start time field. For example, suppose the following records are processed with start_date as blacklist:

| id | start_date | name | email         |
| -- | ---------- |------|---------------|
| 1  | 0          | John | bcd@abc.com   |
| 1  | 10         | John | bcd@abc.com   |
| 1  | 100        | Sam  | abc@abc.com   |

After deduplicating, the resulting records will be:

| id | start_date | name | email         |
| -- | ---------- |------|---------------|
| 1  | 0          | John | bcd@abc.com   |
| 1  | 100        | Sam  | abc@abc.com   |

**Blacklist:** Blacklist for fields to ignore to compare when deduplicating the record.

**Number of Partitions:** Number of partitions to use when grouping the data. This number determines the level of
parallelism for the job. A reasonable starting point is to divide your cluster memory by the pipeline executor memory and
set that as the number of partitions. If not specified. If not specified, 200 is used as default.

Example
-------
For example, Suppose the plugin is configured to use the 'id' field as the key, and receives the following input records, which is the union of records received 
from the existing SCD2 dataset and the new data:

| id | start_date | end_date   |
| -- | ---------- | -----------|
| 1  | 0          | 10000000   |
| 2  | 10         | 20000000   |
| 1  | 100000000  | 500000000  |
| 2  | 21000000   | 10000000   |
| 1  | 10000000   | null       |
| 2  | 15000000   | null       |


The records are grouped by id, then sorted by start_date. The end dates are updated to be N - 1, where N is the start date timestamp 
of the next record.

| id | start_date | end_date            |
| -- | ---------- | --------------------|
| 1  | 0          | 9999999             |
| 1  | 10000000   | 99999999            |
| 1  | 100000000  | 253402214400000000  |
| 2  | 10         | 14999999            |
| 2  | 15000000   | 20999999            |
| 2  | 21         | 253402214400000000  |
