#!/bin/bash

# Generate Android signing key for release builds
# This script creates a keystore for signing Android APKs

KEYSTORE_NAME="release.keystore"
KEY_ALIAS="ipmsg-release"
KEY_PASSWORD=""
STORE_PASSWORD=""
VALIDITY=10000
COUNTRY="CN"
STATE="Beijing"
CITY="Beijing"
ORG="IPMsg"
ORG_UNIT="Development"
NAME="IPMsg Developer"

# Check if keystore already exists
if [ -f "$KEYSTORE_NAME" ]; then
    echo "Keystore already exists at $KEYSTORE_NAME"
    echo "Please delete it first if you want to generate a new one"
    exit 1
fi

# Generate keystore
keytool -genkeypair \
    -v \
    -keystore "$KEYSTORE_NAME" \
    -alias "$KEY_ALIAS" \
    -keyalg RSA \
    -keysize 2048 \
    -validity "$VALIDITY" \
    -storepass "$STORE_PASSWORD" \
    -keypass "$KEY_PASSWORD" \
    -dname "CN=$NAME, OU=$ORG_UNIT, O=$ORG, L=$CITY, ST=$STATE, C=$COUNTRY"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Keystore generated successfully!"
    echo "  File: $KEYSTORE_NAME"
    echo "  Alias: $KEY_ALIAS"
    echo ""
    echo "IMPORTANT: Store this keystore securely!"
    echo "You will need it to update your app in the future."
    echo ""
    echo "Add these to your gradle.properties or local.properties:"
    echo "  RELEASE_STORE_FILE=./build/$KEYSTORE_NAME"
    echo "  RELEASE_STORE_PASSWORD=$STORE_PASSWORD"
    echo "  RELEASE_KEY_ALIAS=$KEY_ALIAS"
    echo "  RELEASE_KEY_PASSWORD=$KEY_PASSWORD"
else
    echo "Failed to generate keystore"
    exit 1
fi
