# SCD2 SparkCompute plugin


Description
-----------
This plugin groups the records by the specified key and sort them based on the start date field
and compute the end date based on that. The end date is computed as the start date of the next value minus 1 second. If
there is no next value, 9999-12-31 is used.

Properties
----------
**Key:** The name of the key field. The records will be grouped based on the key. This field must be a boolean,
int, long, float, double, bytes or string type.

**Start Date Field:** The name of the start date field. The grouped records are sorted based on this field. This must be a timestamp.

**End Date Field:** The name of the end date field. The sorted results are iterated to compute the value of this field based
on the start date.

**End Date Offset:** The offset to compute the end date. The end date will be computed as the next start date minus this offset.
The format is expected to be a number followed by an 'us', 'ms', 's', 'm', 'h', or 'd' specifying the time unit,
with 'us' for microseconds, 'ms' for milliseconds, 's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days.

**Fill In Null:** Fill in null fields from most recent previous records with same id. For example, suppose three records with same key
are processed:

| id | start_date | name | email         |
| -- | ---------- |------|---------------|
| 1  | 0          | John | bcd@abc.com   |
| 1  | 10         | null | abc@abc.com   |
| 1  | 100        | null | null          |

After filling in null, the resulting records will be:

| id | start_date | end_date           | name | email         |
| -- | ---------- |--------------------|------|---------------|
| 1  | 0          | 9                  | John | bcd@abc.com   |
| 1  | 10         | 99                 | John | abc@abc.com   |
| 1  | 100        | 253402214400000000 | John | abc@abc.com   |

**Deduplicate:** Deduplicate consecutive records that have no changes. This can be used with the blacklist to exclude some fields from comparison.
The blacklist typically includes the start time field. If Perserve Target is set to false, the record with the latest record will be picked.
The end time of a record will be computed as the start time of the next non-deduplicated record minus the offset.
For example, suppose the following records are processed with start_date as blacklist:

| id | start_date | name | email         |
| -- | ---------- |------|---------------|
| 1  | 0          | John | bcd@abc.com   |
| 1  | 10         | John | bcd@abc.com   |
| 1  | 100        | Sam  | abc@abc.com   |

After deduplicating, the resulting records will be:

| id | start_date | end_date           | name | email         |
| -- | ---------- |--------------------|------|---------------|
| 1  | 0          | 99                 | John | bcd@abc.com   |
| 1  | 100        | 253402214400000000 | Sam  | abc@abc.com   |

**Blacklist:** Blacklist for fields to ignore to compare when deduplicating the record.

**Preserve Target** Preserve all the records from the scd2 target table when deduplicating.

**Target Field** The name of the field that tells whether the record is coming from the scd2 target table. This field must be boolean.
For example, suppose the target field is set to 'is_target' and the following records are processed:

| id | start_date | name | isTarget      |
| -- | ---------- |------|---------------|
| 1  | 0          | John | false         |
| 1  | 10         | John | true          |
| 1  | 100        | John | false         |
| 1  | 1000       | Sam  | false         |
| 1  | 10000      | Sam  | false         |

After deduplicating and computing the end time, the resulting records will be:

| id | start_date | end_date           | name | isTarget      |
| -- | ---------- |--------------------|------|---------------|
| 1  | 10         | 9999               | John | true          |
| 1  | 10000      | 253402214400000000 | Sam  | false         |

**Number of Partitions:** Number of partitions to use when grouping the data. This number determines the level of
parallelism for the job. A reasonable starting point is to divide your cluster memory by the pipeline executor memory and
set that as the number of partitions. If not specified. If not specified, 200 is used as default.

Example
-------
For example, Suppose the plugin is configured to use the 'id' field as the key, and receives the following input records:

| id | start_date | end_date   |
| -- | ---------- | -----------|
| 1  | 0          | 10000000   |
| 2  | 10         | 20000000   |
| 1  | 100000000  | 500000000  |
| 2  | 21000000   | 10000000   |
| 1  | 10000000   | null       |
| 2  | 15000000   | null       |


The records are grouped by id, then sorted by start_date. The end dates are updated to be N - offset(here assuming offset is 1),
where N is the start date timestamp of the next record.

| id | start_date | end_date            |
| -- | ---------- | --------------------|
| 1  | 0          | 9999999             |
| 1  | 10000000   | 99999999            |
| 1  | 100000000  | 253402214400000000  |
| 2  | 10         | 14999999            |
| 2  | 15000000   | 20999999            |
| 2  | 21         | 253402214400000000  |