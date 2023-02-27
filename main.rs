use aws_lambda_events::s3::S3Event;
use flate2::bufread::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use flate2::GzBuilder;
use lambda_runtime::{handler_fn, Error};
use serde_json::Value;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let func = handler_fn(handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(req: Value, _ctx: lambda_runtime::Context) -> Result<(), Box<Error>> {
    let events: S3Event = serde_json::from_value(req).unwrap();
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
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();

        let decoder: BufReader<GzDecoder<&[u8]>> = BufReader::new(GzDecoder::new(&data[..]));

        let output = Vec::new();
        let mut encoder: GzEncoder<Vec<u8>> = GzBuilder::new()
            .filename("tab_converted.txt")
            .write(output, Compression::default());

        for line in decoder.lines().skip(1) {
            let line = line.unwrap();
            let date: &&str = &line[0..14].trim();
            let serial_number: &&str = &line[15..35].trim();
            let model: &&str = &line[36..78].trim();
            let capacity_bytes: &&str = &line[79..97].trim();
            let failure: &&str = &line[98..108].trim();
            writeln!(
                &mut encoder,
                "{}\t{}\t{}\t{}\t{}",
                date, serial_number, model, capacity_bytes, failure
            )
            .unwrap();
        }
        let output = encoder.finish().expect("failed to flush bytes to file");

        let remote_uri: &String = &key.replace("fixed_width_raw/", "tab_converted/");
        s3_client
            .put_object()
            .bucket(&bucket_name)
            .key(remote_uri)
            .body(output.into())
            .send()
            .await
            .unwrap();
    }
    Ok(())
}
