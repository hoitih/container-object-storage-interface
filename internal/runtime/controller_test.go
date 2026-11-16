/* Copyright 2026 The Kubernetes Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package runtime

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"

	v1alpha1 "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
)

type eventTrackingClient struct {
	kubeclientset.Interface
	eventNamespaces chan string
}

func (c *eventTrackingClient) CoreV1() corev1.CoreV1Interface {
	return &eventTrackingCoreClient{CoreV1Interface: c.Interface.CoreV1(), eventNamespaces: c.eventNamespaces}
}

type eventTrackingCoreClient struct {
	corev1.CoreV1Interface
	eventNamespaces chan string
}

func (c *eventTrackingCoreClient) Events(namespace string) corev1.EventInterface {
	c.eventNamespaces <- namespace
	return c.CoreV1Interface.Events(namespace)
}

func TestRunRecordsEventsAcrossNamespaces(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "objectstorage")

	fakeClient := fake.NewSimpleClientset()
	client := &eventTrackingClient{Interface: fakeClient, eventNamespaces: make(chan string, 10)}
	leaseNamespace := make(chan string, 1)
	events := make(chan k8stesting.CreateAction, 10)
	fakeClient.PrependReactor("get", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		select {
		case leaseNamespace <- action.GetNamespace():
		default:
		}
		return false, nil, nil
	})
	fakeClient.PrependReactor("create", "events", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create := action.(k8stesting.CreateAction)
		if create.GetObject().(*v1.Event).Reason == "TestCrossNamespace" {
			events <- create
		}
		return true, create.GetObject(), nil
	})

	c, err := NewObjectStorageControllerWithClientset("test", "lock", 1, workqueue.DefaultControllerRateLimiter(), client, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.initialized = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	select {
	case ns := <-leaseNamespace:
		if ns != "objectstorage" {
			t.Errorf("leader election namespace = %q, want objectstorage", ns)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for leader election")
	}

	select {
	case ns := <-client.eventNamespaces:
		if ns != "" {
			t.Errorf("event sink namespace = %q, want empty namespace", ns)
		}
	default:
		t.Fatal("no event sink registered")
	}
	select {
	case ns := <-client.eventNamespaces:
		t.Errorf("unexpected second event sink in namespace %q", ns)
	default:
	}

	c.eventRecorder.Event(&v1alpha1.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{Name: "access", Namespace: "dxp-hugo"},
	}, v1.EventTypeNormal, "TestCrossNamespace", "test event")
	select {
	case event := <-events:
		if ns := event.GetNamespace(); ns != "dxp-hugo" {
			t.Errorf("event namespace = %q, want dxp-hugo", ns)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for BucketAccess event")
	}
}
