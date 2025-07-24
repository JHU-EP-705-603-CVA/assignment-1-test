#! /bin/bash

echo "Updating module..."
git submodule update --remote --merge

echo "Staging changes..."
git add .

echo "Updating your repository..."
git push