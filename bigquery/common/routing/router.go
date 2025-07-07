// Copyright (c) 2025 Skyflow, Inc.

package routing

import (
	"github.com/gin-gonic/gin"
)

func CreateRouter() (*gin.Engine, error) {
	// Use release mode for better performance
	// This can be removed during development if debug-level logging is desired
	gin.SetMode(gin.ReleaseMode)

	// Create engine and attach recovery middleware
	router := gin.New()
	router.Use(gin.Recovery())

	// Disable the trusted proxies feature as we do not use client IP
	// https://github.com/gin-gonic/gin/blob/v1.10.0/docs/doc.md#dont-trust-all-proxies
	if err := router.SetTrustedProxies(nil); err != nil {
		return nil, err
	}

	return router, nil
}
