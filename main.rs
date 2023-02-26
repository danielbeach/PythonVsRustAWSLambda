use lambda_runtime::{handler_fn, Error};
use aws_lambda_events::s3::S3Event;
use serde_json::Value;
use flate2::read::{GzDecoder};
use flate2::write::GzEncoder;
use flate2::GzBuilder;
use flate2::Compression;
use std::io::Read;
use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;


#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let func = handler_fn(handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(req: Value, _ctx: lambda_runtime::Context) -> Result<(), Box<Error>> {
    let events: S3Event = serde_json::from_value(req).unwrap();;
    for e in events.records {
        let bucket_name: String = e.s3.bucket.name.expect("Unable to get s3 bucket name.");
        let key: String = e.s3.object.key.expect("unable to get s3 file key");
        
        let config: aws_config::SdkConfig = aws_config::load_from_env().await;
        let s3_client: aws_sdk_s3::Client = aws_sdk_s3::Client::new(&config);
        let data: aws_lambda_events::bytes::Bytes = s3_client
                                    .get_object()
                                    .bucket(&bucket_name)
                                    .key(&key)
                                    .send()
                                    .await.unwrap().body
                                    .collect().await.unwrap().into_bytes();
        
        let mut d: GzDecoder<&[u8]> = GzDecoder::new(&data[..]);
        let mut csv_data: String = String::new();
        d.read_to_string(&mut csv_data).unwrap();

        let split: std::str::Lines<'_> = csv_data.lines();
        let result_vector: Vec<&str> = split.collect();

        let mut tab_converted: String = String::new();
        for line in result_vector.iter().skip(1) {
            let date: &&str = &line[0..14].trim();
            let serial_number: &&str = &line[15..35].trim();
            let model: &&str = &line[36..78].trim();
            let capacity_bytes: &&str = &line[79..97].trim();
            let failure: &&str = &line[98..108].trim();
            let tab_line: String = format!( "{}\t{}\t{}\t{}\t{}\n", date, serial_number, model, capacity_bytes, failure);
            tab_converted.push_str(&tab_line);
            
        }
        let f: File = File::create("/tmp/file.gz").expect("failed to create file");
        let mut gz: GzEncoder<File> = GzBuilder::new()
                .filename("tab_converted.txt")
                .write(f, Compression::default());
        gz.write_all(tab_converted.as_bytes()).expect("failed to write bytes to file");
        gz.finish().expect("failed to flush bytes to file");

        let file: File = File::open("/tmp/file.gz").expect("problem reading file");
        let mut reader: BufReader<File> = BufReader::new(file);
        let mut buffer: Vec<u8> = Vec::new();
        
        reader.read_to_end(&mut buffer).expect("error");

        let remote_uri: &String = &key.replace("fixed_width_raw/", "tab_converted/");
        s3_client.put_object().bucket(&bucket_name).key(remote_uri).body(buffer.into()).send().await.unwrap();
        
    }
    Ok(())
}

