// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package v2

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// ListProcessor lists all processors in the ticdc cluster.
// Usage:
// curl -X GET http://127.0.0.1:8300/api/v2/processors
// Note: It is not useful in new arch cdc, we implement it for compatibility with old arch cdc only.
func (h *OpenAPIV2) ListProcessor(c *gin.Context) {
	prcInfos := make([]ProcessorCommonInfo, 0)

	c.JSON(http.StatusOK, toListResponse(c, prcInfos))
}

// GetProcessor gets a processor in the ticdc cluster.
// Usage:
// curl -X GET http://127.0.0.1:8300/api/v2/{changefeed_id}/{capture_id}
// Note: It is not useful in new arch cdc, we implement it for compatibility with old arch cdc only.
func (h *OpenAPIV2) GetProcessor(c *gin.Context) {
	processorDetail := ProcessorDetail{}
	c.JSON(http.StatusOK, processorDetail)
}
