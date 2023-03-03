# PythonVsRustAWSLambda
Testing the runtime difference between Python and Rust for AWS Lambda.

All AWS Lambda's must include a handler function `lambda_handler`, this is what
the lambda runtime will call be default. 

When triggering these lambdas off an s3 event, it's important to note more
than one event might run though that lambda invocation, hence the
inital loop.

See the full blog post here https://www.confessionsofadataguy.com/aws-lambdas-python-vs-rust-performance-and-cost-savings/

`Rust` lambda.
You will need to add the crate `cargo-lambda`. It helps you package and build
the bootstrip binary needed for deployment.
To build the `bootstrap.zip` that your Rust AWS Lambda needs, run `cargo lambda build --release --output-format zip`
