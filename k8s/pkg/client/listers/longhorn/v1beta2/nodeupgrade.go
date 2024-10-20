/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by lister-gen. DO NOT EDIT.

package v1beta2

import (
	v1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// NodeUpgradeLister helps list NodeUpgrades.
type NodeUpgradeLister interface {
	// List lists all NodeUpgrades in the indexer.
	List(selector labels.Selector) (ret []*v1beta2.NodeUpgrade, err error)
	// NodeUpgrades returns an object that can list and get NodeUpgrades.
	NodeUpgrades(namespace string) NodeUpgradeNamespaceLister
	NodeUpgradeListerExpansion
}

// nodeUpgradeLister implements the NodeUpgradeLister interface.
type nodeUpgradeLister struct {
	indexer cache.Indexer
}

// NewNodeUpgradeLister returns a new NodeUpgradeLister.
func NewNodeUpgradeLister(indexer cache.Indexer) NodeUpgradeLister {
	return &nodeUpgradeLister{indexer: indexer}
}

// List lists all NodeUpgrades in the indexer.
func (s *nodeUpgradeLister) List(selector labels.Selector) (ret []*v1beta2.NodeUpgrade, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1beta2.NodeUpgrade))
	})
	return ret, err
}

// NodeUpgrades returns an object that can list and get NodeUpgrades.
func (s *nodeUpgradeLister) NodeUpgrades(namespace string) NodeUpgradeNamespaceLister {
	return nodeUpgradeNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// NodeUpgradeNamespaceLister helps list and get NodeUpgrades.
type NodeUpgradeNamespaceLister interface {
	// List lists all NodeUpgrades in the indexer for a given namespace.
	List(selector labels.Selector) (ret []*v1beta2.NodeUpgrade, err error)
	// Get retrieves the NodeUpgrade from the indexer for a given namespace and name.
	Get(name string) (*v1beta2.NodeUpgrade, error)
	NodeUpgradeNamespaceListerExpansion
}

// nodeUpgradeNamespaceLister implements the NodeUpgradeNamespaceLister
// interface.
type nodeUpgradeNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all NodeUpgrades in the indexer for a given namespace.
func (s nodeUpgradeNamespaceLister) List(selector labels.Selector) (ret []*v1beta2.NodeUpgrade, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1beta2.NodeUpgrade))
	})
	return ret, err
}

// Get retrieves the NodeUpgrade from the indexer for a given namespace and name.
func (s nodeUpgradeNamespaceLister) Get(name string) (*v1beta2.NodeUpgrade, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1beta2.Resource("nodeupgrade"), name)
	}
	return obj.(*v1beta2.NodeUpgrade), nil
}
