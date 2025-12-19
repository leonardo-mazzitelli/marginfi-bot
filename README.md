# Mary 
**The next-generation friendly neighborhood Marginfi Liquidator**

# Configure
Environment variables are used to configure the application, making it easier to integrate with cloud services. The [template.env](template.env) outlines the environment variables needed to configure the application.

> Local Docker: the [mary.Dockerfile](mary.Dockerfile) contains the Docker configuration for running the application locally.

# Build
tbd

# Run
- Main liquidator flow: `cargo run`
- Quick Geyser connectivity probe: source your `.env` (at minimum `GEYSER_ENDPOINT` and `GEYSER_X_TOKEN`) and run `cargo run --bin geyser_probe`. The probe creates a short-lived subscription that only listens for Solana clock updates, making it a fast way to verify whether your Yellowstone provider credentials work before launching the full service. Use `GEYSER_PROBE_TIMEOUT_SEC` to tweak the wait time if needed.
