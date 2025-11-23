#!/bin/bash
# Quick start script for running the Snowflake data pipeline
# Usage: ./scripts/run_pipeline.sh [step]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

STEP=${1:-all}

echo "=========================================="
echo "Snowflake Data Pipeline"
echo "=========================================="
echo ""

case $STEP in
    setup)
        echo "Running Snowflake setup..."
        python scripts/setup/run_setup.py
        ;;
    ingestion)
        echo "Running ingestion..."
        python scripts/orchestration/pipeline_orchestrator.py --step ingestion
        ;;
    transformation)
        echo "Running transformation..."
        python scripts/orchestration/pipeline_orchestrator.py --step transformation
        ;;
    validation)
        echo "Running validation..."
        python scripts/orchestration/pipeline_orchestrator.py --step validation
        ;;
    all)
        echo "Running full pipeline..."
        python scripts/orchestration/pipeline_orchestrator.py --step all
        ;;
    *)
        echo "Usage: $0 [setup|ingestion|transformation|validation|all]"
        exit 1
        ;;
esac

echo ""
echo "Pipeline execution completed!"

