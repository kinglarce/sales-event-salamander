# Vivenu Events Ticket Scrapper v2

## Overview

This is the modern, refactored version of the Vivenu Events Ticket Scrapper, built with senior software engineering best practices. The v2 system provides a clean, configurable, and maintainable architecture for data ingestion and processing.

## Key Features

### 🏗️ **Clean Architecture**
- **Modular design**: Clean separation of concerns
- **Pipeline-based**: Configurable execution pipelines
- **Docker-ready**: Optimized for containerized deployment
- **Shared components**: Reuses models, utils, and SQL from root level

### 🔧 **Configurable Pipeline System**
- **Default pipeline**: Standard ingestion workflow
- **Custom pipelines**: YAML-based pipeline configuration
- **Dependency management**: Define step dependencies and execution order
- **Timeout handling**: Configurable timeouts for each step
- **Error handling**: Stop-on-failure and retry mechanisms

### 📊 **Enhanced Monitoring & Logging**
- **Structured logging**: Consistent logging across all modules
- **Performance tracking**: Built-in performance monitoring
- **Step tracking**: Real-time progress monitoring
- **Error reporting**: Comprehensive error handling and reporting

### 🚀 **Docker Integration**
- **Docker-first**: Primary deployment method
- **Environment variables**: Full environment variable support
- **Logging**: Integrated with Docker logging
- **Resource management**: Optimized for container environments

## Architecture

```
v2/
├── run_ingest.py              # Main v2 orchestration script
├── ingest_*.py               # Clean ingestion scripts (no _v2 suffix)
├── core/                     # Core modules
│   ├── __init__.py           # Core package initialization
│   ├── config.py             # Configuration management
│   ├── logging.py            # Logging setup
│   ├── database.py           # Database management
│   ├── http_client.py        # HTTP client factory
│   ├── batch_processor.py    # Batch processing
│   └── pipeline.py           # Pipeline management
├── pipeline_configs/         # Pipeline configuration files
│   ├── default.yaml          # Default pipeline
│   ├── minimal.yaml          # Minimal pipeline
│   └── parallel.yaml         # Parallel pipeline
├── examples/                 # Usage examples
└── README_V2.md             # This file
```

## Quick Start

### Using Docker (Recommended)

```bash
# Run v2 system with default pipeline
docker exec -it vivenu-app python main.py --version v2

# Run v2 system with custom pipeline
docker exec -it vivenu-app python main.py --version v2 --pipeline-name minimal

# Run v2 system with debug logging
docker exec -it vivenu-app python main.py --version v2 --debug
```

### Direct Execution

```bash
# Run v2 system
python v2/run_ingest.py

# Run with custom pipeline
python v2/run_ingest.py --pipeline-config v2/pipeline_configs/custom.yaml

# Run with predefined pipeline
python v2/run_ingest.py --pipeline-name minimal
```

## Pipeline Configuration

### Default Pipeline

The default pipeline (`v2/pipeline_configs/default.yaml`) includes:

```yaml
name: "default_ingestion"
description: "Default ingestion pipeline with all steps"
parallel_execution: false
stop_on_failure: true
retry_failed_steps: true
max_retries: 3

steps:
  - name: "static_data_ingestion"
    function: "static_data_ingestion"
    enabled: true
    timeout: 300.0
    depends_on: []

  - name: "events_tickets_ingestion"
    function: "events_tickets_ingestion"
    enabled: true
    timeout: 1800.0
    depends_on: ["static_data_ingestion"]

  - name: "analytics_processing"
    function: "analytics_processing"
    enabled: true
    timeout: 300.0
    depends_on: ["events_tickets_ingestion"]
```

### Available Pipelines

- **default**: Full ingestion pipeline with all steps
- **minimal**: Essential steps only (static data + events/tickets)
- **parallel**: Parallel execution pipeline

### Custom Pipeline

Create your own pipeline configuration:

```yaml
# v2/pipeline_configs/custom.yaml
name: "custom_ingestion"
description: "Custom ingestion pipeline"
parallel_execution: false
stop_on_failure: true

steps:
  - name: "static_data_ingestion"
    function: "static_data_ingestion"
    enabled: true
    timeout: 300.0
    depends_on: []
    
  - name: "events_tickets_ingestion"
    function: "events_tickets_ingestion"
    enabled: true
    timeout: 1800.0
    depends_on: ["static_data_ingestion"]
```

## Function Mapping

The v2 system maps pipeline functions to actual v1 scripts:

| Function | Script | Description |
|----------|--------|-------------|
| `static_data_ingestion` | `v1/ingest_static_data.py` | Static data setup |
| `events_tickets_ingestion` | `v1/ingest_events_tickets.py` | Events and tickets ingestion |
| `coupon_ingestion` | `v1/ingest_coupons.py` | Coupon processing |
| `age_groups_ingestion` | `v1/ingest_age_groups.py` | Age group processing |
| `gender_fix_ingestion` | `v1/ingest_gender_fix.py` | Gender data fixes |
| `analytics_processing` | `v1/ticket_analytics.py` | Analytics and reporting |

## Command Line Options

### Main Entry Point

```bash
# V2 system
python main.py --version v2

# With custom pipeline
python main.py --version v2 --pipeline-config v2/pipeline_configs/custom.yaml

# With predefined pipeline
python main.py --version v2 --pipeline-name minimal

# With debug logging
python main.py --version v2 --debug
```

### Direct V2 Execution

```bash
# Default pipeline
python v2/run_ingest.py

# Custom pipeline
python v2/run_ingest.py --pipeline-config v2/pipeline_configs/custom.yaml

# Predefined pipeline
python v2/run_ingest.py --pipeline-name minimal

# Debug mode
python v2/run_ingest.py --debug

# Skip API calls
python v2/run_ingest.py --skip-fetch
```

## Environment Variables

The v2 system supports environment variables for Docker deployment:

```bash
# Set version
docker exec -it vivenu-app -e VERSION=v2 python main.py

# Set debug mode
docker exec -it vivenu-app -e DEBUG_MODE=true python main.py --version v2 --debug

# Set custom pipeline
docker exec -it vivenu-app -e PIPELINE_CONFIG=v2/pipeline_configs/custom.yaml python main.py --version v2
```

## Logging

### Log Files

- **Location**: `logs/` directory
- **Format**: `v2_ingest_{timestamp}.log`
- **Levels**: DEBUG, INFO, WARNING, ERROR

### Log Structure

```
2025-10-23 06:41:51,421 - __main__ - INFO - 🚀 Starting V2 Ingest System...
2025-10-23 06:41:51,421 - __main__ - INFO - 📄 Using default pipeline configuration
2025-10-23 06:41:51,421 - __main__ - INFO - 📋 Executing pipeline: default_ingestion
2025-10-23 06:41:51,421 - __main__ - INFO - 🚀 Starting v1/ingest_static_data.py...
2025-10-23 06:41:52,104 - __main__ - INFO - ✅ v1/ingest_static_data.py completed successfully
2025-10-23 06:41:52,104 - __main__ - INFO - ✅ Step static_data_ingestion completed successfully
```

## Error Handling

### Stop on Failure

By default, the pipeline stops if any step fails:

```yaml
stop_on_failure: true
```

### Retry Mechanisms

Configure retry behavior:

```yaml
retry_failed_steps: true
max_retries: 3
```

### Timeout Handling

Each step can have a custom timeout:

```yaml
steps:
  - name: "events_tickets_ingestion"
    timeout: 1800.0  # 30 minutes
```

## Performance

### Step Execution

- **Sequential execution**: Steps run in dependency order
- **Timeout protection**: Each step has configurable timeout
- **Resource management**: Proper cleanup after each step
- **Progress tracking**: Real-time step completion tracking

### Monitoring

- **Step timing**: Track execution time for each step
- **Resource usage**: Monitor memory and CPU usage
- **Error tracking**: Comprehensive error logging
- **Progress reporting**: Real-time progress updates

## Development

### Adding New Steps

1. **Create the script** in the appropriate location
2. **Add function mapping** in `v2/run_ingest.py`:
   ```python
   script_mapping = {
       'your_new_function': 'path/to/your_script.py',
       # ... existing mappings
   }
   ```
3. **Update pipeline configuration** to include the new step
4. **Test the pipeline** with the new step

### Custom Pipeline Creation

1. **Create YAML file** in `v2/pipeline_configs/`
2. **Define pipeline structure**:
   ```yaml
   name: "your_pipeline"
   description: "Your custom pipeline"
   steps:
     - name: "step1"
       function: "function1"
       depends_on: []
   ```
3. **Test the pipeline**:
   ```bash
   python v2/run_ingest.py --pipeline-config v2/pipeline_configs/your_pipeline.yaml
   ```

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure shared components are accessible
2. **Pipeline errors**: Check YAML syntax and function mappings
3. **Timeout errors**: Increase timeout values for slow steps
4. **Dependency errors**: Verify step dependencies are correct

### Debug Mode

Enable debug logging for detailed information:

```bash
python v2/run_ingest.py --debug
```

### Log Analysis

Check log files for detailed error information:

```bash
tail -f logs/v2_ingest_*.log
```

## Migration from v1

### Key Differences

- **Pipeline-based**: v2 uses configurable pipelines instead of hardcoded scripts
- **Clean structure**: No `_v2` suffixes, clean file names
- **Docker-first**: Optimized for Docker deployment
- **Shared components**: Reuses v1 scripts with better orchestration

### Migration Steps

1. **Update imports**: Change from `v1.script` to `v2.script`
2. **Use pipelines**: Replace hardcoded execution with pipeline configuration
3. **Update logging**: Use structured logging instead of print statements
4. **Test thoroughly**: Verify all functionality works with new structure

## Best Practices

### Pipeline Design

- **Keep steps focused**: Each step should have a single responsibility
- **Use dependencies**: Define clear step dependencies
- **Set appropriate timeouts**: Configure realistic timeout values
- **Enable retries**: Use retry mechanisms for transient failures

### Error Handling

- **Stop on failure**: Use `stop_on_failure: true` for critical pipelines
- **Log everything**: Ensure comprehensive logging for debugging
- **Handle timeouts**: Set appropriate timeout values
- **Monitor progress**: Use progress tracking for long-running pipelines

### Performance

- **Optimize steps**: Ensure each step is as efficient as possible
- **Use timeouts**: Prevent hanging steps
- **Monitor resources**: Track memory and CPU usage
- **Clean up**: Ensure proper cleanup after each step

## Support

For issues and questions:

1. **Check logs**: Review log files for error details
2. **Test pipelines**: Verify pipeline configurations
3. **Debug mode**: Use `--debug` for detailed information
4. **Documentation**: Review this README and examples

## Examples

See the `v2/examples/` directory for usage examples and custom pipeline configurations.