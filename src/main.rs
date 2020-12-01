use elasticsearch::Elasticsearch;
use elasticsearch::http::transport::Transport;
use elasticsearch::BulkParts;
use elasticsearch::BulkOperation;
use elasticsearch::indices::IndicesDeleteParts;
use elasticsearch::indices::IndicesCreateParts;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use anyhow::anyhow;
use serde_json::json;

// The data is an array of objects like this one (unused fields omitted)
//   {
//     "datasetid": "collecte-des-sapins-de-noel",
//     "recordid": "ef89fdb5cbb3b397d2988b7d23c1fee5199b989c",
//     "fields": {
//       "commune": "TOULOUSE",
//       "adresse": "88 all Jean Jaurès / angle rue Riquet",
//       "geo_point_2d": [
//         43.6089310498,
//         1.45385907091
//       ]
//     }
//   },

#[derive(Debug, Deserialize)]
struct SourcePlace {
    pub datasetid: String,
    pub recordid: String,
    pub fields: SourceFields,
}

#[derive(Debug, Deserialize)]
struct SourceFields {
    pub commune: String,
    pub adresse: String,
    pub geo_point_2d: (f64, f64), // lat, lon
}

#[derive(Debug, Serialize)]
struct IndexedPlace {
    pub dataset_id: String,
    pub record_id: String,
    pub city: String,
    pub street: String,
    pub location: (f64, f64), // lon, lat
}

const DATA_URL: &str = "https://data.toulouse-metropole.fr/explore/dataset/collecte-des-sapins-de-noel/download/?format=json";
const INDEX_NAME: &str = "xmas-tree-recycling";

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // Use the URL (including login/password) from the ELASTICSEARCH_URL env variable
    let es_url = std::env::var("ELASTICSEARCH_URL")?;
    let es_client = Elasticsearch::new(Transport::single_node(&es_url)?);

    // Delete the existing index, we will overwrite everything (ignore error if the index doesn't exist)
    println!("Cleaning up existing data.");
    es_client.indices()
        .delete(IndicesDeleteParts::Index(&[INDEX_NAME]))
        .send().await?;

    // Create the index with a geo_point for location (use the defaults for other properties)
    println!("Setting up index.");
    es_client.indices().create(IndicesCreateParts::Index(&INDEX_NAME))
        .body(json!({
            "mappings": {
                "properties": {
                    "location": { "type": "geo_point" }
                }
            }
        }))
        .send().await?
        .error_for_status_code()?;

    // Fetch the data
    println!("Fetching xmas tree recycling data.");

    let response = reqwest::get(DATA_URL).await?
        .error_for_status()?;

    // Parse the JSON response
    let places: Vec<SourcePlace> = serde_json::from_slice(&response.bytes().await?)?;

    // Transform each item into the target index format
    let indexed_places = places.into_iter()
        .map(|place| IndexedPlace {
            dataset_id: place.datasetid,
            record_id: place.recordid,
            city: place.fields.commune,
            street: place.fields.adresse,
            location: (
                place.fields.geo_point_2d.1,
                place.fields.geo_point_2d.0
            )
        }
    );

    // And store everything in the "xmas-tree-collect" index
    println!("Storing data.");
    let response = es_client
        .bulk(BulkParts::Index(INDEX_NAME))
        .body(
            // create a bulk indexing operation for each place
            indexed_places.map(|place|
                BulkOperation::from(BulkOperation::index(place))
            ).collect()
        )
        .send().await?
        .error_for_status_code()?;

    // Make sure we don't have bulk ingestion errors
    let bulk_response = response.json::<JsonValue>().await?;

    if bulk_response["errors"] == JsonValue::Bool(true) {
        return Err(anyhow!("Failed to store data: {}", bulk_response));
    }

    // All good!
    println!("Done!");

    Ok(())
}
