use aws_lambda_events::event::sns::SnsEvent;
use lambda_runtime::{error::HandlerError, lambda};
use log::{self, error};
use rusoto_core::Region;
use rusoto_dynamodb::{DynamoDb, DynamoDbClient, GetItemInput, PutItemInput, UpdateItemInput};
use serde_json::json;
use simple_logger;
use std::error::Error;
use rusoto_xray::{XRay, XRayClient, PutTraceSegmentsInput};

fn dynamodb_client() -> DynamoDbClient {
    DynamoDbClient::new(Region::default())
}

fn extract_names(object_key: &str) -> (String, String) {
    let key_parts: Vec<&str> = object_key.split('/').collect();
    let bar_name = key_parts[key_parts.len() - 2].to_string();
    let drink_name_with_extension = key_parts.last().unwrap().to_string();
    let drink_name = drink_name_with_extension.split('.').next().unwrap().to_string();
    (bar_name, drink_name)
}

fn handler(event: SnsEvent) -> Result<(), HandlerError> {
    for record in event.records {
        if let Some(s3_event) = record.s3 {
            let bucket_name = s3_event.bucket.name;
            let object_key = s3_event.object.key.unwrap();
            let object_url = format!("https://{}.s3.amazonaws.com/{}", bucket_name, object_key);

            let (bar_name, drink_name) = extract_names(&object_key);

            // Create an X-Ray subsegment for DynamoDB operation
            let xray_client = XRayClient::new(Region::default());
            let segment = xray_client.get_trace_id().sync().unwrap().trace_id.unwrap();
            let subsegment = xray_client.put_trace_segments(PutTraceSegmentsInput {
                trace_segment_documents: vec![format!(
                    r#"{{ "name": "DynamoDB Interaction", "trace_id": "{}" }}"#,
                    segment
                )],
            }).sync().unwrap();

            let client = dynamodb_client();
            let get_item_input = GetItemInput {
                table_name: "drink_images".to_string(),
                key: json!({
                    "barName": bar_name.clone(),
                    "drinkName": drink_name.clone()
                }).into(),
                ..Default::default()
            };

            match client.get_item(get_item_input).sync() {
                Ok(output) => {
                    if let Some(item) = output.item {
                        let update_item_input = UpdateItemInput {
                            table_name: "drink_images".to_string(),
                            key: json!({
                                "barName": bar_name.clone(),
                                "drinkName": drink_name.clone()
                            }).into(),
                            update_expression: Some("SET s3ObjectKey = :val1".to_string()),
                            expression_attribute_values: Some(json!({
                                ":val1": object_url.clone()
                            }).as_object().unwrap().clone()),
                            ..Default::default()
                        };
                        if let Err(e) = client.update_item(update_item_input).sync() {
                            error!("Error updating item: {}", e);
                        }
                    } else {
                        let put_item_input = PutItemInput {
                            table_name: "drink_images".to_string(),
                            item: json!({
                                "barName": bar_name.clone(),
                                "drinkName": drink_name.clone(),
                                "s3ObjectKey": object_url.clone()
                            }).into(),
                            ..Default::default()
                        };
                        if let Err(e) = client.put_item(put_item_input).sync() {
                            error!("Error putting item: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Error getting item: {}", e);
                }
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(handler);
    Ok(())
}
