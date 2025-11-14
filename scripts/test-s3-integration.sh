#!/bin/bash

# Script to run S3 integration tests with Localstack
#
# This script:
#   1. Starts Localstack container
#   2. Waits for Localstack to be healthy
#   3. Runs S3 integration tests
#   4. Cleans up Localstack container
#
# Usage:
#   ./scripts/test-s3-integration.sh

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== DittoFS S3 Integration Tests ===${NC}\n"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    echo "Please start Docker and try again"
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: docker command not found${NC}"
    exit 1
fi

echo -e "${YELLOW}Step 1: Starting Localstack...${NC}"

# Start Localstack using docker compose
docker compose -f docker-compose.test.yml up -d

# Wait for Localstack to be healthy
echo -e "${YELLOW}Step 2: Waiting for Localstack to be ready...${NC}"
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker compose -f docker-compose.test.yml ps | grep -q "healthy"; then
        echo -e "${GREEN}Localstack is ready!${NC}\n"
        break
    fi

    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "${RED}Error: Localstack failed to become healthy${NC}"
        docker compose -f docker-compose.test.yml logs
        docker compose -f docker-compose.test.yml down
        exit 1
    fi

    echo "Waiting for Localstack... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

# Run the integration tests
echo -e "${YELLOW}Step 3: Running S3 integration tests...${NC}\n"

# Set environment variable for Localstack endpoint
export LOCALSTACK_ENDPOINT=http://localhost:4566

# Run tests with integration build tag
if go test -v -tags=integration ./pkg/content/s3/...; then
    echo -e "\n${GREEN}✓ All S3 integration tests passed!${NC}"
    TEST_EXIT_CODE=0
else
    echo -e "\n${RED}✗ S3 integration tests failed${NC}"
    TEST_EXIT_CODE=1
fi

# Cleanup
echo -e "\n${YELLOW}Step 4: Cleaning up Localstack...${NC}"
docker compose -f docker-compose.test.yml down

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "\n${GREEN}=== Test Complete: SUCCESS ===${NC}"
else
    echo -e "\n${RED}=== Test Complete: FAILED ===${NC}"
fi

exit $TEST_EXIT_CODE
