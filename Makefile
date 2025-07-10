# Copyright (c) 2025 Skyflow, Inc.

.PHONY: add-copyright

add-copyright:
# add #-style comments
	find . -type f -name "*.tf" -o -name "Makefile" -o -name "Dockerfile" -o -name "*.sh" | xargs -I {} ./scripts/add-copyright.sh {} "# "
# add //-style comments
	find . -type f -name "*.go" -o -name "*.java" | xargs -I {} ./scripts/add-copyright.sh {} "// "
