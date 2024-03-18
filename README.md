#### BigQuery Table Fetch Data And Insert Data

Note : Make sure `gcp.json` exists

Make a note of the following before running the program

- GCP_PROJECT
- GCP_DATASET
- GCP_BQ_TABLE

`gcp.json` should exist in the same directory where `./mdata` is executed (OR) it should exist here : `/etc/secrets/gcp.json`

```bash
go build -o mdata
```

Run the program (It will listen on port 9900)

```bash
./mdata
```

> Part-1: Fetching

```bash
curl -X GET -H "accept:application/json" -H "content-type:application/json" "http://localhost:9900/api/v1/bq/{GCP_PROJECT}/{GCP_DATASET}/{GCP_BQ_TABLE}" 2>/dev/null | python3 -m json.tool 
```

> Part-2: Inserting Records

Note: JSON must be an array of one or more records

Generate Random JSON Payload First.

```bash
python3.10 generate_random_data.py > payload.json
```

Insert Records

In this example, the BigQuery table has two columns (created through the GCP Console)

```
BigQuery column-1 : scrape_ts (type TIMESTAMP)
BigQuery column-2 : payload   (type JSON) 
```

```bash
curl -X POST "http://localhost:9900/api/v1/bq/{GCP_PROJECT}/{GCP_DATASET}/{GCP_BQ_TABLE}" -H "accept:application/json" -H "content-type:application/json" -d@payload.json
```

Example-1 of Payload  (`payload.json`) > Only 1 record will be inserted (when HTTP POST call is made)

Note: `payload.json` should be an JSON array (even if it is just one record)

```json
[
    {
        "payload":
        {
            "x1": "v1",
            "x2": "v2",
            "y1": 10.10,
            "y2":
            [
                1,
                2,
                3,
                4,
                5
            ]
        }
    }
]
```

Example-2 of Payload  (`payload.json`) > Only 2 records will be inserted (when HTTP POST call is made)

Note: `payload.json` should be an JSON array (even if it is just one record)

```json
[
    {
        "id": "fa1a3cb7-f142-442c-9edd-c7cf8e27b51b",
        "type": "type2",
        "value": 55
    },
    {
        "id": "b2c36b01-c6f4-487f-82a5-882fc6cc99df",
        "type": "type1",
        "value": 36
    }
]
```