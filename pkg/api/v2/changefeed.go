// Copyright 2022 PingCAP, Inc.
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
	"context"
	"fmt"
	"strconv"

	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/api/internal/rest"
	"github.com/pingcap/ticdc/pkg/common"
)

// ChangefeedsGetter has a method to return a ChangefeedInterface.
type ChangefeedsGetter interface {
	Changefeeds() ChangefeedInterface
}

// ChangefeedInterface has methods to work with Changefeed items.
// We can also mock the changefeed operations by implement this interface.
type ChangefeedInterface interface {
	// Create creates a changefeed
	Create(ctx context.Context, cfg *v2.ChangefeedConfig) (*v2.ChangeFeedInfo, error)
	// VerifyTable verifies table for a changefeed
	VerifyTable(ctx context.Context, cfg *v2.VerifyTableConfig) (*v2.Tables, error)
	// Update updates a changefeed
	Update(ctx context.Context, cfg *v2.ChangefeedConfig,
		namespace string, name string) (*v2.ChangeFeedInfo, error)
	// Resume resumes a changefeed with given config
	Resume(ctx context.Context, cfg *v2.ResumeChangefeedConfig, namespace string, name string) error
	// Delete deletes a changefeed by name
	Delete(ctx context.Context, namespace string, name string) error
	// Pause pauses a changefeed with given name
	Pause(ctx context.Context, namespace string, name string) error
	// Get gets a changefeed detaail info
	Get(ctx context.Context, namespace string, name string) (*v2.ChangeFeedInfo, error)
	// List lists all changefeeds
	List(ctx context.Context, namespace string, state string) ([]v2.ChangefeedCommonInfo, error)
	// Move Table to target node, it just for make test case now. **Not for public use.**
	MoveTable(ctx context.Context, namespace string, name string, tableID int64, targetNode string) error
	// Move dispatchers in a split Table to target node, it just for make test case now. **Not for public use.**
	MoveSplitTable(ctx context.Context, namespace string, name string, tableID int64, targetNode string) error
	// split table based on region count, it just for make test case now. **Not for public use.**
	SplitTableByRegionCount(ctx context.Context, namespace string, name string, tableID int64) error
	// merge table, it just for make test case now. **Not for public use.**
	MergeTable(ctx context.Context, namespace string, name string, tableID int64) error
}

// changefeeds implements ChangefeedInterface
type changefeeds struct {
	client rest.CDCRESTInterface
}

// newChangefeed returns changefeeds
func newChangefeeds(c *APIV2Client) *changefeeds {
	return &changefeeds{
		client: c.RESTClient(),
	}
}

func (c *changefeeds) Create(ctx context.Context,
	cfg *v2.ChangefeedConfig,
) (*v2.ChangeFeedInfo, error) {
	result := &v2.ChangeFeedInfo{}
	err := c.client.Post().
		WithURI("changefeeds").
		WithBody(cfg).
		Do(ctx).Into(result)
	return result, err
}

func (c *changefeeds) VerifyTable(ctx context.Context,
	cfg *v2.VerifyTableConfig,
) (*v2.Tables, error) {
	result := &v2.Tables{}
	err := c.client.Post().
		WithURI("verify_table").
		WithBody(cfg).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *changefeeds) Update(ctx context.Context,
	cfg *v2.ChangefeedConfig, namespace string, name string,
) (*v2.ChangeFeedInfo, error) {
	result := &v2.ChangeFeedInfo{}
	u := fmt.Sprintf("changefeeds/%s?namespace=%s", name, namespace)
	err := c.client.Put().
		WithURI(u).
		WithBody(cfg).
		Do(ctx).
		Into(result)
	return result, err
}

// Resume a changefeed
func (c *changefeeds) Resume(ctx context.Context,
	cfg *v2.ResumeChangefeedConfig, namespace string, name string,
) error {
	u := fmt.Sprintf("changefeeds/%s/resume?namespace=%s", name, namespace)
	return c.client.Post().
		WithURI(u).
		WithBody(cfg).
		Do(ctx).Error()
}

// Delete a changefeed
func (c *changefeeds) Delete(ctx context.Context,
	namespace string, name string,
) error {
	u := fmt.Sprintf("changefeeds/%s?namespace=%s", name, namespace)
	return c.client.Delete().
		WithURI(u).
		Do(ctx).Error()
}

// Pause a changefeed
func (c *changefeeds) Pause(ctx context.Context,
	namespace string, name string,
) error {
	u := fmt.Sprintf("changefeeds/%s/pause?namespace=%s", name, namespace)
	return c.client.Post().
		WithURI(u).
		Do(ctx).Error()
}

// Get gets a changefeed detaail info
func (c *changefeeds) Get(ctx context.Context,
	namespace string, name string,
) (*v2.ChangeFeedInfo, error) {
	err := common.ValidateChangefeedID(name)
	if err != nil {
		return nil, err
	}
	result := new(v2.ChangeFeedInfo)
	u := fmt.Sprintf("changefeeds/%s?namespace=%s", name, namespace)
	err = c.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return result, err
}

// List lists all changefeeds
func (c *changefeeds) List(ctx context.Context,
	namespace string, state string,
) ([]v2.ChangefeedCommonInfo, error) {
	result := &v2.ListResponse[v2.ChangefeedCommonInfo]{}
	err := c.client.Get().
		WithURI("changefeeds?namespace="+namespace).
		WithParam("state", state).
		Do(ctx).
		Into(result)
	return result.Items, err
}

// MoveTable to target node, it just for make test case now. **Not for public use.**
func (c *changefeeds) MoveTable(ctx context.Context,
	namespace string, name string, tableID int64, targetNode string,
) error {
	url := fmt.Sprintf("changefeeds/%s/move_table?namespace=%s", name, namespace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		WithParam("targetNodeID", targetNode).
		Do(ctx).Error()
	return err
}

// move dispatchers in a split table to target node, it just for make test case now. **Not for public use.**
func (c *changefeeds) MoveSplitTable(ctx context.Context,
	namespace string, name string, tableID int64, targetNode string,
) error {
	url := fmt.Sprintf("changefeeds/%s/move_split_table?namespace=%s", name, namespace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		WithParam("targetNodeID", targetNode).
		Do(ctx).Error()
	return err
}

// SplitTableByRegionCount split table based on region count, it just for make test case now. **Not for public use.**
func (c *changefeeds) SplitTableByRegionCount(ctx context.Context,
	namespace string, name string, tableID int64,
) error {
	url := fmt.Sprintf("changefeeds/%s/split_table_by_region_count?namespace=%s", name, namespace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		Do(ctx).Error()
	return err
}

// MergeTable merge table, it just for make test case now. **Not for public use.**
func (c *changefeeds) MergeTable(ctx context.Context,
	namespace string, name string, tableID int64,
) error {
	url := fmt.Sprintf("changefeeds/%s/merge_table?namespace=%s", name, namespace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		Do(ctx).Error()
	return err
}
