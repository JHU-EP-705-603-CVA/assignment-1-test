#! /bin/bash

echo "Updating module..."
git submodule update --remote --merge

echo "Staging changes..."
git add ./test

echo "Committing changes..."
git commit -m "Updating testing modules"

echo "Updating your repository..."
git push